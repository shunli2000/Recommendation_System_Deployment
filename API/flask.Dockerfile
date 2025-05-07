FROM python:3.11-slim

WORKDIR /app
RUN mkdir API

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    g++ \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY API/requirements.txt .
RUN pip install --upgrade pip wheel setuptools
RUN pip install --no-cache-dir -r requirements.txt

COPY API/app.py ./API

EXPOSE 8083