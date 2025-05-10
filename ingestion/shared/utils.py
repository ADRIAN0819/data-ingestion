import os, time, requests, boto3

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "data-ingestion/1.0"})

def iterate_pages(url, per_page=100, retries=3):
    next_url = f"{url}?page=1&page_size={per_page}"
    while next_url:
        for attempt in range(retries):
            try:
                r = SESSION.get(next_url, timeout=10)
                r.raise_for_status()
                break
            except requests.RequestException:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)  # back-off exponencial
        data = r.json()
        yield data
        next_url = data.get("next")

def upload_to_s3(local, bucket, key):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    s3.upload_file(
        local, bucket, key,
        ExtraArgs={"ContentType": "text/csv", "ACL": "private"}
    )
    print(f"✔  Subido {key} → s3://{bucket}")
