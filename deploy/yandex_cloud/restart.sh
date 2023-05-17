#!/bin/bash

bot_name=${1?"Please provide bot name. Example: $0 lstm"} # arg1 is a bot name

user="yc-user"
public_ip="$(yc compute instance list | grep biml-trade-bots | awk '{print $10}')"
biml_vm_dir="/home/$user/biml"

echo "Restarting bot $bot_name at $public_ip machine"
set +e
ssh $user@"$public_ip" "cd $biml_vm_dir ; sudo docker-compose stop $bot_name"
ssh $user@"$public_ip" "cd $biml_vm_dir ; sudo docker-compose start $bot_name &"
