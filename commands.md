kcat -b localhost:9092 -t movielog7 -C -o end
curl http://128.2.205.109:8082/recommend/1024
kubectl get svc recommender-service
minikube tunnel

echo "🚨 Deleting all deployments, services, and pods..."
kubectl get deployments
kubectl delete deployment --all

kubectl get svc
kubectl delete svc --all

kubectl get pods
kubectl delete pod --all

echo "✅ Kubernetes resources deleted"

docker build -t flask-recommender:latest -f API/Dockerfile .

minikube stop

minikube delete

minikube start --driver=docker

kubectl apply -f k8s/deployment.yaml

kubectl apply -f k8s/service.yaml

minikube mount /home/pochunch/mlip/group-project-s25-the-inceptionists/ML/models:/models

sudo setcap cap_net_admin+ep $(which minikube)

docker-compose down --volumes --remove-orphans
docker-compose build --no-cache
docker-compose build
docker-compose up

docker exec -it group-project-s25-the-inceptionists_load-balancer_1 /bin/bash

docker ps

docker-compose logs model-service1
docker-compose logs model-service2
docker network inspect backend

sudo -u postgres psql
