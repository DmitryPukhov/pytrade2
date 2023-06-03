#!/bin/bash

bot_name=${1?"Please provide bot name. Example: $0 lstmstrategy"} # arg1 is a bot name

user="yc-user"
public_ip="$(yc compute instance list | grep pytrade2 | awk '{print $10}')"
pytrade2_vm_dir="/home/$user/pytrade2"

echo "Restarting bot $bot_name at $public_ip machine"
set +e
ssh $user@"$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose stop $bot_name"
ssh $user@"$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose start $bot_name &"
