#!/bin/bash


. deploy_lib.sh
bot_names=${*:-"$DEFAULT_BOT_NAMES"}


echo "Reconfiguring $bot_names at $VM_PUBLIC_IP machine."

####### main #######
echo "Stopping remote dockers of $bot_names"
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker stop $bot_names"

prepare_tmp_config
copy_to_remote_config

echo "Starting remote dockers or $bot_names"
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose up $bot_names &"

## Follow logs
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose logs -n100 -f"