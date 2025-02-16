#!/bin/bash

# Read and automatically export env variables
set -a
source .env
source common_lib.sh
ensure_namespace
set +a

# Global vars
PROJECT_DIR=../..

TAG="pytrade2"
REGISTRY=$(minikube ip):5000
PUSH_TAG="$REGISTRY/$TAG"

function build_pytrade2(){
  # Build
  echo "Building docker: $TAG"
  cd $PROJECT_DIR
  docker build -t $TAG .
  cd "$OLDPWD"
}

function push_pytrade2(){
  # Push
  echo "Pushing docker image to $PUSH_TAG"
  docker tag $TAG $PUSH_TAG
  docker push "$PUSH_TAG"
  echo "Pushed"
}



###############
# main
###############
# Exit on error
set -e
build_pytrade2
push_pytrade2
