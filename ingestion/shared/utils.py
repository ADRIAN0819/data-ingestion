import os, time, requests, boto3
from urllib.parse import urlparse, urlencode, urlunparse, parse_qs

# ────────── Sesión HTTP reutilizable ──────────
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "data-ingestion/1.0"})

# ────────── Iterador de páginas flexible ──────
def iterate_pages(url, per_page=100, retries=5, timeout=30):
    """
    Itera sobre un endpoint REST devolviendo listas de registros, ya sea que la
    API responda:
        • Lista JSON          → [ {...}, {...} ]
        • Paginación estilo DRF → {"results":[...], "next":"…"}
    
    Parámetros
    ----------
    url : str
        Endpoint base.
    per_page : int
        Tamaño de página al usar paginación. Ignorado si la API no lo soporta.
    retries : int
        Veces a reintentar cada llamada con back-off exponencial (2**n s).
    timeout : int
        Segundos máximos por petición. 0 ó None → sin límite.
    """
    if not url:
        raise ValueError("URL no puede ser vacía")

    # Construye ?page_size=N conservando cualquier query previa
    parts = list(urlparse(url))
    query = parse_qs(parts[4])
    query.setdefault("page_size", [str(per_page)])
    parts[4] = urlencode(query, doseq=True)
    next_url = urlunparse(parts)

    # None o 0 → requests sin timeout
    effective_timeout = None if not timeout else timeout

    while next_url:
        # ––– reintentos con back-off –––
        for attempt in range(retries):
            try:
                resp = SESSION.get(next_url, timeout=effective_timeout)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)

        data = resp.json()

        # Caso 1: lista plana
        if isinstance(data, list):
            yield data
            break

        # Caso 2: dict paginado
        if isinstance(data, dict):
            yield data.get("results", [])
            next_url = data.get("next")  # None ⇒ termina el while
        else:
            raise ValueError(f"Formato de respuesta inesperado: {type(data)}")

# ────────── Subida a S3 ────────────────────────
def upload_to_s3(local, bucket, key):
    """
    Sube un archivo al bucket S3 con ACL privada y Content-Type CSV.
    Requiere que las variables AWS_* existan o la instancia tenga un IAM Role.
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token     = os.getenv("AWS_SESSION_TOKEN"),
        region_name           = os.getenv("AWS_REGION", "us-east-1"),
    )
    s3.upload_file(
        local, bucket, key,
        ExtraArgs={"ContentType": "text/csv", "ACL": "private"}
    )
    print(f"✔  Subido {key} → s3://{bucket}")
