#!/bin/bash
# Read and automatically export env variables
set -a
source .env
set +a

# All secrets
function redeploy_secrets(){
  for yaml_file in secret/*.yaml; do
      echo "Deploying secret $yaml_file"
      set +e
      secret_name=$(basename "${yaml_file%.*}")
      kubectl delete secret "$secret_name"
      set -e
      # Deploy the secret
      kubectl apply -f "$yaml_file"
  done
}

# Mlflow run, tracking, postgresql
function redeploy_mlflow(){
  echo "Redeploying mlflow"
  # Delete previous ignoring errors if not exists
  set +e
  helm delete pytrade2-mlflow
  kubectl delete pvc data-pytrade2-mlflow-postgresql-0
  kubectl delete pvc pytrade2-mlflow-tracking
  set -e

  # Install mlflow
  helm install pytrade2-mlflow oci://registry-1.docker.io/bitnamicharts/mlflow --values mlflow/values.yaml
}

function redeploy_prometheus(){
  echo "Redeploying prometheus"
  set +e
  helm delete pytrade2-prometheus
  kubectl delete pvc pytrade2-prometheus-0
  set -e

  helm install pytrade2-prometheus oci://registry-1.docker.io/bitnamicharts/prometheus --values prometheus/values.yaml --set serviceMonitor.enabled=true
}

# Grafana
function redeploy_grafana(){
  echo "Redeploying grafana"
  # Delete previous ignoring errors if not exists
  set +e
  helm delete pytrade2-grafana
  kubectl delete pvc pytrade2-grafana
  set -e

  # install grafana
  helm install pytrade2-grafana oci://registry-1.docker.io/bitnamicharts/grafana --values grafana/values.yaml
}

###############
# main
###############
# Exit on error
set -e

redeploy_secrets
redeploy_mlflow
redeploy_grafana
redeploy_prometheus
