#!/bin/bash


. deploy_lib.sh
bot_names=${*:-"$DEFAULT_BOT_NAMES"}

# Ssh to cloud and follow logs
ssh "$VM_USER"@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose logs -n 100 --follow $bot_names &"
