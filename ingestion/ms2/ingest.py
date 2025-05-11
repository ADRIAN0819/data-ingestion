import os, pandas as pd, logging
from ingestion.shared.utils import upload_to_s3, iterate_pages

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

BUCKET  = os.getenv("S3_BUCKET")
PREFIX  = "ms2"          # ← carpeta en S3

RESOURCES = {
    "consultas":    os.getenv("MS2_CONSULTAS"),
    "tratamientos": os.getenv("MS2_TRATAMIENTOS"),
}

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", 30))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", 5))

def fetch(url):
    rows = []
    for chunk in iterate_pages(url, retries=HTTP_RETRIES, timeout=HTTP_TIMEOUT):
        rows.extend(chunk)
    return pd.json_normalize(rows, sep="_")

def main() -> None:
    os.makedirs("/app/data", exist_ok=True)

    for name, url in RESOURCES.items():
        if not url:
            logging.warning("%s: URL no definida, se omite", name)
            continue

        try:
            logging.info("Descargando %s…", name)
            df = fetch(url)

            subdir = f"/app/data/{name}"
            os.makedirs(subdir, exist_ok=True)
            local = f"{subdir}/{name}.csv"

            df.to_csv(local, index=False)
            upload_to_s3(local, BUCKET, f"{PREFIX}/{name}.csv")
        except Exception as exc:
            logging.error("%s: error %s – no se sube este recurso", name, exc)


if __name__ == "__main__":
    main()
