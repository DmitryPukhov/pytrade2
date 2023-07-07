#!/bin/bash

default_bot_names="lstmstrategy2 lstmstrategy simplekerasstrategy"
bot_names=${*:-"$default_bot_names"}

. deploy_lib.sh



echo "Reconfiguring $bot_names at $VM_PUBLIC_IP machine."

####### main #######
echo "Stopping remote dockers"
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose stop $bot_names"

prepare_tmp_config
copy_to_remote_config

echo "Starting remote dockers"
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose start $bot_names &"

## Follow logs
ssh "$VM_USER@$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose logs -n100 -f"
