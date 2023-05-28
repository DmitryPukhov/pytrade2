#!/bin/bash

default_bot_names="lstm2 lstm simplekeras"
bot_names=${*:-"$default_bot_names"}

echo "Deploying $bot_names"

user="yc-user"
public_ip="$(yc compute instance list | grep pytrade2 | awk '{print $10}')"
pytrade2_vm_dir="/home/$user/pytrade2"

dockercomposeup() {
  echo "Starting bot $bot_names at $public_ip machine"
  ssh $user@"$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose up $bot_names &"
}

#### Main
./stop_all.sh
./build_all.sh
dockercomposeup
