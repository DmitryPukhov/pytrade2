#!/bin/bash

bot_name=${1?"Please provide bot name. Example: $0 lstm"} # arg1 is a bot name

user="yc-user"
public_ip="$(yc compute instance list | grep biml-trade-bots | awk '{print $10}')"
biml_vm_dir="/home/$user/biml"

restart() {
  echo "Restarting bot $bot_name at $public_ip machine"
  ssh $user@"$public_ip" "cd $biml_vm_dir ; sudo docker-compose up $bot_name &"
}

echo "Redeploying all bots then starting $bot_name"
./stop_all.sh
./build_all.sh
restart
