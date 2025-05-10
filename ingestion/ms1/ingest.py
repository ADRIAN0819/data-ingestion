import os, pandas as pd, logging
from ingestion.shared.utils import upload_to_s3, iterate_pages   # 👈 ruta nueva

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

BUCKET = os.getenv("S3_BUCKET")
PREFIX = "ms1"

RESOURCES = {
    "mascotas":     os.getenv("MS1_MASCOTAS"),
    "propietarios": os.getenv("MS1_PROPIETARIOS"),
}

def fetch(url):
    rows = []
    for page in iterate_pages(url):
        rows.extend(page["results"])
    return pd.json_normalize(rows, sep='_')

def main():
    os.makedirs("/app/data", exist_ok=True)

    for name, url in RESOURCES.items():
        if not url:
            logging.warning("%s: URL no definida, se omite", name)
            continue

        logging.info("Descargando %s…", name)
        df = fetch(url)

        local = f"/app/data/{name}.csv"
        df.to_csv(local, index=False)

        upload_to_s3(local, BUCKET, f"{PREFIX}/{name}.csv")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Fallo inesperado")
        raise
