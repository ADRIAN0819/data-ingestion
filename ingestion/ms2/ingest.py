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

def main():
    os.makedirs("/app/data", exist_ok=True)

    for name, url in RESOURCES.items():
        if not url:
            logging.warning("%s: URL no definida; se omite", name)
            continue
        try:
            logging.info("Descargando %s…", name)
            df = fetch(url)

            # ───── NUEVO: subcarpeta local por recurso ─────
            local_dir = f"/app/data/{name}"
            os.makedirs(local_dir, exist_ok=True)   # crea p.ej. /app/data/mascotas
            local = f"{local_dir}/{name}.csv"

            df.to_csv(local, index=False)

            # ───── NUEVO: subcarpeta en S3 ─────────────────
            s3_key = f"{PREFIX}/{name}/{name}.csv"   # ms1/mascotas/mascotas.csv
            upload_to_s3(local, BUCKET, s3_key)

        except Exception as exc:
            logging.error("%s: error %s – no se sube", name, exc)



if __name__ == "__main__":
    main()
