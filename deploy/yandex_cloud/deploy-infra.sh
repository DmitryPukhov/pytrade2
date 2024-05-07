#!/bin/bash
### Deploy infrastructure: mlflow to cloud

. deploy_lib.sh
VM_MLFLOW_DIR=$VM_PYTRADE2_DIR/deploy/docker/mlflow
VM_GRAFANA_DIR=$VM_PYTRADE2_DIR/deploy/docker/prometheus-grafana

#### Main
echo "Stopping mlflow at $VM_PUBLIC_IP machine"
ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_MLFLOW_DIR ; sudo docker-compose down"
ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_VM_GRAFANA_DIR ; sudo docker-compose down"

prepare_tmp
copy_to_remote

echo "Starting mlflow at $VM_PUBLIC_IP machine"
ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_MLFLOW_DIR ; sudo docker-compose up -d"
ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_GRAFANA_DIR ; sudo docker-compose up -d"
