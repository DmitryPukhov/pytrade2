#!/bin/bash

# Read and automatically export env variables
set -a
source yc.env
set +a

# Global vars
PROJECT_DIR=../..
ENV_FILE=yc.env
TAG="pytrade2"
PUSH_TAG="$REGISTRY/$TAG"
NAMESPACE="default"
BUILD_SERVER_POD="build-server"
APP_NAME="pytrade2"
BUILD_SERVER_PROJECT_DIR="/opt/$APP_NAME"

# Prepare docker image for all pytrade2 apps
function build_push_pytrade2(){
  # Build
  echo "Building docker: $TAG"
  cd $PROJECT_DIR
  docker build -t $TAG .
  docker tag $TAG $PUSH_TAG
  cd "$OLDPWD"

  # Push
  echo "Pushing docker image to $PUSH_TAG"
  docker push $PUSH_TAG
}

# pytrade2 app deployment
function redeploy_pytrade2(){
  app_name="pytrade2-app"
  # Delete old
  set +e
  helm delete $app_name
  set -e
  # Install new
  helm install $app_name ./pytrade2 --values yc.env
}

set_namespace(){
  [ -z "$NAMESPACE" ] &&  { echo "Error: namespace variable is not set"; exit 1; }
  echo "Set namespace to $NAMESPACE"
  kubectl config set-context --current --namespace="$NAMESPACE"
  echo "Current namespace: $(kubectl config view --minify --output 'jsonpath={..namespace}')"
}

build_server_delete(){
  # Delete
  echo "Delete old pod: $BUILD_SERVER_POD"
  kubectl delete pod $BUILD_SERVER_POD #--grace-period=0 --force
  # Wait
  while kubectl get pod $BUILD_SERVER_POD &> /dev/null; do
    echo "Waiting for the pod $BUILD_SERVER_POD to be deleted..."
    sleep 5
  done
}

# Create build server pod if does not exist
build_server_ensure(){
  # Check existing
  echo "Looking for existing pod $BUILD_SERVER_POD"
  if [ -n "$(kubectl get pods | grep $BUILD_SERVER_POD)" ]; then
    # Found existing, return, don't create
    echo  "Build server pod: $BUILD_SERVER_POD found."
    return
  fi

  # Create
  echo "Build server pod: $BUILD_SERVER_POD not found. Creating it.";
  echo "Create pod: $BUILD_SERVER_POD"
  # Run docker in docker pod
  kubectl run $BUILD_SERVER_POD --privileged --image=docker:dind

 # Wait until created
  echo "Waiting until the pod $BUILD_SERVER_POD has been created"
  kubectl wait --for=condition=Ready pod/$BUILD_SERVER_POD
  kubectl get pods $BUILD_SERVER_POD
  # Install docker
  echo "Pod $BUILD_SERVER_POD created."
}

# Copy project from local machine to build server
copy_project_to_pod(){
  echo "Copy $APP_NAME artifacts of app: $APP_NAME from: $PROJECT_DIR to: $BUILD_SERVER_POD:$BUILD_SERVER_PROJECT_DIR"
  # Clean remote dir
  kubectl exec "$BUILD_SERVER_POD" -- /bin/sh -c "rm -r -f $BUILD_SERVER_PROJECT_DIR"
  kubectl exec "$BUILD_SERVER_POD" -- /bin/sh -c "mkdir -p $BUILD_SERVER_PROJECT_DIR"

  # Copy files to build server
  kubectl cp "$PROJECT_DIR/pytrade2/" "$BUILD_SERVER_POD:$BUILD_SERVER_PROJECT_DIR/pytrade2/"  # Copy app
  kubectl exec "$BUILD_SERVER_POD" -- /bin/sh -c "rm -f '$BUILD_SERVER_PROJECT_DIR/pytrade2/resources/app-dev.yaml'"
  kubectl cp "$PROJECT_DIR/pyproject.toml" "$BUILD_SERVER_POD:$BUILD_SERVER_PROJECT_DIR" # Copy pyproject.toml
  kubectl cp "$PROJECT_DIR/Dockerfile" "$BUILD_SERVER_POD:$BUILD_SERVER_PROJECT_DIR" # Copy dockerfile
  kubectl cp "$PROJECT_DIR/deploy/k8s/build_server.sh" "$BUILD_SERVER_POD:$BUILD_SERVER_PROJECT_DIR" # Copy build script
  kubectl cp "$PROJECT_DIR/deploy/k8s/$ENV_FILE" "$BUILD_SERVER_POD:$BUILD_SERVER_PROJECT_DIR/.env" # Copy build script
}

build_server_build_push(){
  echo "Remote build at $BUILD_SERVER_POD docker image $TAG"
  kubectl exec "$BUILD_SERVER_POD" -- sh -c "cd $BUILD_SERVER_PROJECT_DIR && sh ./build_server.sh"
 }


###############
# main
###############
# Exit on error
set -e

build_server_ensure
copy_project_to_pod
build_server_build_push

#build_server_delete

