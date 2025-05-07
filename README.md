# 🎬 Fianl Project: Recommendation System Deployment

This repository contains our end-to-end production-ready movie recommendation system developed as part of the AI Engineering final project at CMU. The system ingests real-time user behavior, serves personalized recommendations, supports online experimentation, and monitors performance and fairness across the ML pipeline.

## 🔧 Engineering Highlights

- Built and deployed a **Flask API behind a Python load balancer on Kubernetes**, enabling scalable and fault-tolerant inference.
- Set up **CI/CD pipelines** with Docker and Jenkins to ensure consistent, automated deployments.
- Automated **model tracking and experiment management** using MLflow.
- **Monitored performance and reliability** using Grafana dashboards, with **proactive alerting** to detect and address system issues in real time.
- Followed full **Software Development Life Cycle (SDLC)** best practices, including Git version control, peer reviews, and automated quality checks.

## 🛠️ Key Features

- **Real-Time Data:** Kafka stream of watch logs, ratings, and metadata for 1M users and 27K movies  
- **Evaluation:** Offline metrics, online A/B testing, and fairness analysis  
- **MLOps:** MLflow for experiment tracking and retraining  
- **Infrastructure:** PostgreSQL, Docker (with `docker-compose`), Jenkins CI/CD, and Grafana monitoring

## 📦 Installation

We recommend using Python `3.12.3`. To install the required packages:

```bash
pip install -r requirements.txt
```

## 🚀 Running the Application

To run the application, use the following command. Change the model with the argument:

```bash
python API/app.py --model 1
```

## ⚙️ Architecture
Pipeline: Kafka → Data Validation → PostgreSQL → Model Training → API Inference

Deployment: Flask API + Python Load Balancer on Kubernetes

Monitoring: Prometheus + Grafana for real-time metrics and alerting

Automation: Docker + Jenkins for CI/CD and containerized workflows

Experimentation: MLflow for model versioning and reproducibility

## 🙌 Team
Po-Chun Chen, Jiahao Guo, Anushka Bhave, Shun Li, Gilbert Wu