FROM python:3.11-slim
WORKDIR /app

COPY ingestion/ms3/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/shared/ /app/ingestion/shared/
COPY ingestion/ms3/     /app/ingestion/ms3/

ENV PYTHONPATH=/app
CMD ["python", "-u", "ingestion/ms3/ingest.py"]

