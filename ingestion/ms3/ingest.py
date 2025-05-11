import os, pandas as pd, logging
from ingestion.shared.utils import upload_to_s3, iterate_pages

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

BUCKET  = os.getenv("S3_BUCKET")
PREFIX  = "ms3"

RESOURCES = {
    "images": os.getenv("MS3_IMAGES"),
}

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", 60))   # quizá más alto
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", 6))

def fetch(url):
    rows = []
    for chunk in iterate_pages(url, retries=HTTP_RETRIES, timeout=HTTP_TIMEOUT):
        rows.extend(chunk)
    return pd.json_normalize(rows, sep="_")

def main():
    os.makedirs("/app/data", exist_ok=True)
    for name, url in RESOURCES.items():
        if not url:
            logging.warning("%s: URL no definida; se omite", name)
            continue
        try:
            logging.info("Descargando %s…", name)
            df = fetch(url)
            local = f"/app/data/{name}.csv"
            df.to_csv(local, index=False)
            upload_to_s3(local, BUCKET, f"{PREFIX}/{name}.csv")
        except Exception as exc:
            logging.error("%s: error %s – no se sube", name, exc)

if __name__ == "__main__":
    main()

