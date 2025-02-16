#!/bin/bash

# Read and automatically export env variables
set -a
source .env
source common_lib.sh
ensure_namespace
set +a

# Global vars
PROJECT_DIR=../..
DEPLOY_ENV_PATH="$PROJECT_DIR/minikube.env"
TMP_DIR="$PROJECT_DIR/deploy/k8s/tmp"
CONFIGMAP_PATH="$TMP_DIR/configmap.yaml"

# Prepare configmap with environment variables
function create_env_configmap(){
  # Ensure directory without previous env files
  mkdir -p $(dirname CONFIGMAP_PATH)
  rm $CONFIGMAP_PATH

  # Check source env file exists
  if [ ! -f "$DEPLOY_ENV_PATH" ]; then
    echo "File not found: $DEPLOY_ENV_PATH"
    exit 1
  fi

  # Configmap header
  printf "apiVersion: v1\nkind: ConfigMap\nmetadata:\n    name: pytrade2-env\ndata:\n" >> $CONFIGMAP_PATH

  # Read source env file line by line, convert and write to confitmap
  while IFS='=' read -r key value; do
    # Пропускаем комментарии и пустые строки
    if [[ -z "$key" || $key =~ ^\s*#.*$ ]]; then
      continue
    fi

    # Экранируем значение от возможных символов, которые могут нарушить формат YAML
    value="${value//\"/\\\"}"
    printf "    \"$key\": \"$value\"\n" >> $CONFIGMAP_PATH
  done < $DEPLOY_ENV_PATH

  echo "Created successfully: $CONFIGMAP_PATH"
}

function redeploy_pytrade2_chart(){
  app_name="pytrade2"
  eval $(minikube docker-env)
  # Delete old
  set +e
  helm delete $app_name
  set -e
  # Install new
  echo "Installing $app_name with environment: $CONFIGMAP_PATH"
  kubectl apply -f $CONFIGMAP_PATH
  echo "Applied $CONFIGMAP"
  helm install $app_name ./pytrade2

}

###############
# main
###############
# Exit on error
set -e

create_env_configmap
redeploy_pytrade2_chart
