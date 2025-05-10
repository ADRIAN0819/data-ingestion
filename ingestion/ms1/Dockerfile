FROM python:3.11-slim

WORKDIR /app

# Copiamos requirements del propio microservicio
COPY ingestion/ms1/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el código que necesita: ms1 + shared
COPY ingestion/shared/ /app/ingestion/shared/
COPY ingestion/ms1/ /app/ingestion/ms1/

# Aseguramos que Python vea /app
ENV PYTHONPATH=/app

CMD ["python", "-u", "ingestion/ms1/ingest.py"]
