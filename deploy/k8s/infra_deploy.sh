#!/bin/bash

function redeploy_secrets(){
  for yaml_file in secret/*.yaml; do
      echo "Deploying secret $yaml_file"
      secret_name=$(basename "${yaml_file%.*}")
      kubectl delete secret "$secret_name"
      kubectl apply -f "$yaml_file"
  done
}

function redeploy_mlflow(){
  echo "Redeploying mlflow"
  helm delete pytrade2-mlflow
  kubectl delete pvc data-pytrade2-mlflow-postgresql-0

  # pvc
  #kubectl delete pvc pytrade2-mlflow-tracking
  #kubectl get pvc

  helm install pytrade2-mlflow oci://registry-1.docker.io/bitnamicharts/mlflow --values mlflow/values.yaml
}

function redeploy_grafana(){
  echo "Redeploying grafana"
  helm delete pytrade2-grafana
  kubectl delete pvc pytrade2-grafana
  helm install pytrade2-grafana oci://registry-1.docker.io/bitnamicharts/grafana --values grafana/values.yaml
}

redeploy_secrets
#redeploy_mlflow
redeploy_grafana
