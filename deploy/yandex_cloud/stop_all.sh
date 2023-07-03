#!/bin/bash

. deploy_lib.sh

echo "Stopping pytrade2 at $VM_PUBLIC_IP machine"
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose down"

