from flask import Flask, request
import os
import json
import requests
import logging
import hashlib
from google.cloud import storage, bigquery

print("Starting Flask app...")

app = Flask(__name__)

# Variabili ambiente
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
BQ_DATASET = os.environ.get("BQ_DATASET")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_first_dataset_id(obj, path="root"):
    """Cerca ricorsivamente la prima chiave che contiene 'datasetId'."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if "datasetid" in k.lower() and isinstance(v, str):
                return v.strip(), path + "." + k
            found = find_first_dataset_id(v, path + "." + k)
            if found:
                return found
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            found = find_first_dataset_id(item, path + f"[{i}]")
            if found:
                return found
    return None


def safe_int(val):
    try:
        return int(val) if val is not None else None
    except ValueError:
        return None


def generate_id_from_trustpilot(rec):
    base_str = f"{rec.get('datePublished')}_{rec.get('authorName')}_{rec.get('reviewBody')}"
    return hashlib.md5(base_str.encode('utf-8')).hexdigest()


# --- Funzioni di mapping per canale ---
def map_record_gmb(rec):
    return {
        "source": "gmb",
        "review_id": str(rec.get("reviewId") or "").strip(),
        "date": rec.get("publishedAtDate"),
        "title": rec.get("title"),
        "rating": safe_int(rec.get("stars")),
        "text": rec.get("text"),
        "username": rec.get("name"),
    }


@app.route("/", methods=["POST"])
def webhook_handler():
    # --- Verifica secret ---
    secret = request.args.get("secret")
    if secret != WEBHOOK_SECRET:
        logger.error("❌ Secret errato nel webhook")
        return "Unauthorized", 403

    # --- Recupera source ---
    source_param = request.args.get("source")
    if not source_param:
        return "Missing source", 400

    # --- Ricezione payload ---
    try:
        data = request.get_json(force=True)
        logger.info(f"📩 Payload ricevuto:\n{json.dumps(data, indent=2, ensure_ascii=False)}")
    except Exception as e:
        logger.exception("❌ Errore parsing JSON")
        return f"Invalid JSON: {e}", 400

    # --- Estrazione datasetId ---
    dataset_id = None
    dataset_path = None
    if data.get("defaultDatasetId"):
        dataset_id = data["defaultDatasetId"].strip()
        dataset_path = "root.defaultDatasetId"
    elif (data.get("payload") or {}).get("defaultDatasetId"):
        dataset_id = data["payload"]["defaultDatasetId"].strip()
        dataset_path = "root.payload.defaultDatasetId"
    elif (data.get("resource") or {}).get("defaultDatasetId"):
        dataset_id = data["resource"]["defaultDatasetId"].strip()
        dataset_path = "root.resource.defaultDatasetId"
    else:
        found = find_first_dataset_id(data)
        if found:
            dataset_id, dataset_path = found

    if not dataset_id:
        logger.error("❌ Dataset ID non trovato nel payload Apify")
        return "Missing dataset ID", 400

    logger.info(f"✅ Dataset ID trovato: {dataset_id} (percorso: {dataset_path})")

    # --- Scarica dataset da Apify ---
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&format=json"
    logger.info(f"⬇️ Scarico dati da Apify: {url}")
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        raw_items = resp.json()
    except Exception as e:
        logger.exception("❌ Errore durante il download dal dataset Apify")
        return f"Error fetching dataset from Apify: {e}", 500

    logger.info(f"📦 Numero recensioni scaricate: {len(raw_items)}")

    # --- Applica mapping ---
    if source_param == "gmb":
        mapped = [map_record_gmb(r) for r in raw_items]
    else:
        return f"Unknown source '{source_param}'", 400

    # --- Scrivi su GCS ---
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"test_client_x/{source_param}/{dataset_id}.json"
    ndjson_data = "\n".join(json.dumps(r, ensure_ascii=False) for r in mapped)

    logger.info(f"💾 Scrivo file su GCS: gs://{BUCKET_NAME}/{blob_name}")
    try:
        blob = bucket.blob(blob_name)
        blob.upload_from_string(ndjson_data, content_type="application/json")
    except Exception as e:
        logger.exception("❌ Errore durante il salvataggio su GCS")
        return f"Error saving to GCS: {e}", 500

    # --- Carica in staging ---
    bq_client = bigquery.Client()
    staging_table = f"{BQ_DATASET}.eataly_dev"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("review_id", "STRING"),
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("rating", "INTEGER"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("username", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
        ignore_unknown_values=True,
    )

    try:
        gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
        logger.info(f"📤 LOAD in BigQuery staging: {staging_table} da {gcs_uri}")
        load_job = bq_client.load_table_from_uri(gcs_uri, staging_table, job_config=job_config)
        load_job.result()
        logger.info("✅ Caricamento in staging completato")
    except Exception as e:
        logger.exception("❌ Errore durante il caricamento in BigQuery")
        return f"Errore BigQuery LOAD: {e}", 500

    # --- MERGE ---
    merge_sql = f"""
    MERGE `{bq_client.project}.{BQ_DATASET}.eataly_prod` T
    USING `{bq_client.project}.{BQ_DATASET}.eataly_dev` S
    ON TRIM(T.review_id) = TRIM(S.review_id) AND TRIM(T.source) = TRIM(S.source)
    WHEN NOT MATCHED THEN
      INSERT (source, review_id, date, title, rating, text, username)
      VALUES (S.source, S.review_id, S.date, S.title, S.rating, S.text, S.username)
    """
    try:
        logger.info(f"🔄 Eseguo MERGE nella tabella finale test_client_x")
        bq_client.query(merge_sql).result()
        logger.info("✅ MERGE completato")

        # Pulizia staging
        logger.info("🧹 Svuoto la tabella di staging")
        bq_client.query(f"TRUNCATE TABLE `{BQ_DATASET}.eataly_dev`").result()
    except Exception as e:
        logger.exception("❌ Errore durante il MERGE in BigQuery")
        return f"Errore BigQuery MERGE: {e}", 500

    return f"Processed {len(mapped)} reviews for {source_param}", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
