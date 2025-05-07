FROM python:3.11-slim

WORKDIR /app
RUN mkdir API

COPY API/load_balancer.py ./API

RUN apt-get update && apt-get install -y curl iputils-ping \
    && pip install flask requests prometheus_client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 8082