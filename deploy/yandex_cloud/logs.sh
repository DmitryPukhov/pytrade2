#!/bin/bash

bot_name=$1
. deploy_lib.sh


# Ssh to cloud and follow logs
ssh "$VM_USER"@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose logs -n 100 --follow $bot_name &"
