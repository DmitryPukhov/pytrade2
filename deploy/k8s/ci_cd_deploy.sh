#!/bin/bash

# Read and automatically export env variables
set -a
source .env
set +a


# Create namespace if does not exist
function ensure_namespace() {
  if kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
    echo "Namespace '$NAMESPACE' is found."
  else
    echo "Creating namespace '$NAMESPACE'..."
    kubectl create namespace "$NAMESPACE"
  fi
  kubectl config set-context --current --namespace="$NAMESPACE"
}

function print_jenkins_info() {
  # Print jenkins url
  jenkins_url=$(minikube service jenkins --url -n $NAMESPACE)
  echo "Jenkins url: $jenkins_url"

  # Print admin user and password
  for suffix in "user" "password"; do
    encoded_value=$(kubectl get secret jenkins -n $NAMESPACE -o yaml | grep "jenkins-admin-$suffix:" | sed 's/.*: //')
    decoded_value=$(echo $encoded_value | base64 --decode)
    echo "Jenkins admin $suffix: $decoded_value"
  done
}

function deploy_jenkins() {

  echo "Installing Jenkins..."
  helm repo add jenkins https://charts.jenkins.io
  #helm repo update

  # Pvc
  kubectl apply -f jenkins/pv.yaml
  kubectl apply -f jenkins/pvc.yaml

  # Jenkins reinstall
  helm uninstall jenkins --namespace $NAMESPACE
  helm install jenkins jenkins/jenkins \
    --namespace $NAMESPACE \
    --set controller.serviceType=LoadBalancer \
    --set controller.admin.password="$JENKINS_ADMIN_PASSWORD" \
    --values jenkins/values.yaml \
    #--debug \

    print_jenkins_info
}

function deploy_argo() {
  echo "Installing Argo..."
  kubectl create namespace argocd
  helm repo add argo https://argoproj.github.io/argo-helm

  # Install argo
  kubectl delete -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
  kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

  #  expose the Argo CD API server to external
  kubectl patch svc argocd-server -n argocd -p '{"spec": {"type": "LoadBalancer"}}'

  # Print Argo info
  argo_url=$(minikube service argocd-server --url -n argocd)
  echo "Argo url: $argo_url"
  echo "Argo secret:"
  kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d;echo
}


#ensure_namespace

ensure_namespace
# jenkins repo update is too slow, commented out
#ensure_repo
deploy_jenkins
#deploy_argo
#deploy_argo

#print_jenkins_info

