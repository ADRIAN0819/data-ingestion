import os
import pandas as pd
import logging
from ingestion.shared.utils import upload_to_s3, iterate_pages

# ─────────── Configuración global ────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

BUCKET   = os.getenv("S3_BUCKET")
PREFIX   = "ms1"

# Endpoints
RESOURCES = {
    "mascotas":     os.getenv("MS1_MASCOTAS"),
    "propietarios": os.getenv("MS1_PROPIETARIOS"),
}

# Parámetros HTTP (con valores por defecto)
HTTP_TIMEOUT  = int(os.getenv("HTTP_TIMEOUT", 30))   # seg
HTTP_RETRIES  = int(os.getenv("HTTP_RETRIES", 5))    # intentos

# ─────────── Helpers ─────────────────────────
def fetch(url: str) -> pd.DataFrame:
    """Descarga todos los registros del endpoint y devuelve DataFrame."""
    rows: list[dict] = []
    for chunk in iterate_pages(url, retries=HTTP_RETRIES, timeout=HTTP_TIMEOUT):
        rows.extend(chunk)
    return pd.json_normalize(rows, sep="_")

# ─────────── Main loop ───────────────────────
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


# ─────────── Entry point ─────────────────────
if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Fallo inesperado en el contenedor")
        raise