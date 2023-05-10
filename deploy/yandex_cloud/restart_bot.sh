#!/bin/bash

bot_name=${1?"Please provide bot name.\n Example: $0 lstm"}

user="yc-user"
public_ip="$(yc compute instance list | grep biml-trade-bots | awk '{print $10}')"
biml_vm_dir="/home/$user/biml"

echo "Restarting bot $bot_name at $public_ip machine"
#ssh $user@"$public_ip" "cd $biml_vm_dir ; sudo docker-compose down $bot_name"
ssh $user@"$public_ip" "cd $biml_vm_dir ; sudo docker-compose up $bot_name &"

