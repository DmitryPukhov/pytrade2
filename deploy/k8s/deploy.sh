#!/bin/bash

# Delete previous
helm delete pytrade2-mlflow

helm delete pytrade2-mlflow

kubectl delete pvc data-pytrade2-mlflow-postgresql-0
#kubectl patch pvc mlflow-tracking  -p '{"metadata":{"finalizers":null}}'
#kubectl patch pod mlflow-postgresql-0   -p '{"metadata":{"finalizers":null}}'
kubectl delete pvc pytrade2-mlflow-tracking
kubectl get pvc

#
# Deploy secrets
for yaml_file in secret/*.yaml; do
    echo "Deploying secret $yaml_file"
    secret_name=$(basename "${yaml_file%.*}")
    kubectl delete secret "$secret_name"
    kubectl apply -f "$yaml_file"
done

echo "Deploying mlflow"
helm install pytrade2-mlflow oci://registry-1.docker.io/bitnamicharts/mlflow --values mlflow/values.yaml
