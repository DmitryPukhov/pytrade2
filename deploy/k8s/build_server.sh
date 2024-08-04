#!/bin/bash
#######################
# The script to be run on build server
# docker build push

# exit on error
set -e

# Read and automatically export env variables
set -a
source .env
set +a

# Global vars
APP_NAME="pytrade2"
PUSH_TAG="$REGISTRY/$APP_NAME"
PROJECT_DIR="/opt/$APP_NAME"

# Build
echo "Building docker in $PROJECT_DIR"
cd $PROJECT_DIR
docker build -t $APP_NAME .
docker tag $APP_NAME $PUSH_TAG

# Push to registry
echo "Pushing docker $PUSH_TAG"
docker push $PUSH_TAG

