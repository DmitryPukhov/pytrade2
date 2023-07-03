#!/bin/bash

. deploy_lib.sh

default_bot_names="lstmstrategy2 lstmstrategy simplekerasstrategy"
bot_names=${*:-"$default_bot_names"}

echo "Deploying $bot_names"

dockercomposeup() {
  echo "Starting bot $bot_names at $VM_PUBLIC_UP machine"
  ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose up $bot_names &"
}

#### Main
./stop_all.sh
./build_all.sh
dockercomposeup
