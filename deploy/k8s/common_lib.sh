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