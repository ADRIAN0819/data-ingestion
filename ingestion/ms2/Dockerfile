FROM python:3.11-slim
WORKDIR /app

COPY ingestion/ms2/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/shared/ /app/ingestion/shared/
COPY ingestion/ms2/     /app/ingestion/ms2/

ENV PYTHONPATH=/app
CMD ["python", "-u", "ingestion/ms2/ingest.py"]


