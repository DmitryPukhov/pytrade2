#!/bin/bash

# Read and automatically export env variables
set -a
source .env
set +a

# Global vars
PROJECT_DIR=../..
ENV_FILE=yc.env
TAG="pytrade2:latesti"
REGISTRY=$(minikube ip):5000
PUSH_TAG="$REGISTRY/$TAG"
NAMESPACE="default"
BUILD_SERVER_POD="build-server"
APP_NAME="pytrade2"
BUILD_SERVER_PROJECT_DIR="/opt/$APP_NAME"


# Prepare docker image for all pytrade2 apps
function build_push_pytrade2(){
  # Build
  echo "Building docker: $TAG"
  eval $(minikube docker-env)
  cd $PROJECT_DIR
  docker build -t $TAG .

  # Push
  #echo "Pushing docker image to $PUSH_TAG"
  docker tag $TAG $PUSH_TAG
  docker push "$PUSH_TAG"
  cd "$OLDPWD"
}

# pytrade2 app deployment
function redeploy_pytrade2(){
  app_name="pytrade2-app"
  # Delete old
  set +e
  helm delete $app_name
  set -e
  # Install new
  helm install $app_name ./pytrade2 --values .env
}

set_namespace(){
  [ -z "$NAMESPACE" ] &&  { echo "Error: namespace variable is not set"; exit 1; }
  echo "Set namespace to $NAMESPACE"
  kubectl config set-context --current --namespace="$NAMESPACE"
  echo "Current namespace: $(kubectl config view --minify --output 'jsonpath={..namespace}')"
}


###############
# main
###############
# Exit on error
set -e

build_push_pytrade2
#echo $PUSH_TAG
#build_puserver_ensure
#build_server_ensure
#copy_project_to_pod
#build_server_build_push

#build_server_delete

