#!/bin/bash

bot_name=${1?"Please provide bot name. Example: $0 lstmbidaskregressionstrategy"} # arg1 is a bot name
. deploy_lib.sh

echo "Restarting bot $bot_name at $VM_PUBLIC_IP machine"
set +e
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose stop $bot_name"
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose start $bot_name &"
