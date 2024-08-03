#!/bin/bash

# Read and automatically export env variables
set -a
source yc.env
set +a

PROJECT_DIR=../../
TAG="pytrade2"
PUSH_TAG="$REGISTRY/$TAG"

# Prepare docker image for all pytrade2 apps
function build_push_pytrade2(){
  echo "Building docker: $TAG"
  cd $PROJECT_DIR
  docker build -t $TAG .
  docker tag $TAG $PUSH_TAG
  cd "$OLDPWD"

  echo "Pushing docker image to $PUSH_TAG"
  docker push $PUSH_TAG
}

# pytrade2 app deployment
function redeploy_pytrade2(){
  app_name="pytrade2-app"
  set +e
  helm delete $app_name
  set -e
  helm install $app_name ./pytrade2 --values yc.env
}

###############
# main
###############
# Exit on error
set -e

build_push_pytrade2

