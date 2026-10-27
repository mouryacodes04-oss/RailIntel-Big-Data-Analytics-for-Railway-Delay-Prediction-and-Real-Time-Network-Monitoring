FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PATH=/usr/local/lib/python3.11/site-packages/pyspark/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark==3.5.2 \
    numpy==1.26.4 \
    psycopg2-binary==2.9.10

WORKDIR /app
