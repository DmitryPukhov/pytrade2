#!/bin/bash

bot_name=${1?"Please provide bot name. Example: $0 lstmstrategy"} # arg1 is a bot name

local_cfg="$(pwd)/secret/$bot_name.yaml"
tmp_cfg="$(pwd)/tmp/pytrade2/pytrade2/cfg/$bot_name.yaml"

user="yc-user"
public_ip="$(yc compute instance list | grep pytrade2 | awk '{print $10}')"
pytrade2_vm_dir="/home/$user/pytrade2"
remote_cfg="$user@$public_ip:$pytrade2_vm_dir/pytrade2/cfg/$bot_name.yaml"


echo "Reconfiguring $bot_name at $public_ip machine. Copy $local_cfg to $remote_cfg"

echo "Stopping remote docker"
ssh "$user@$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose stop $bot_name"

echo "Preparing temp config to copy"
cp -f "$local_cfg" "$tmp_cfg"
scp "$tmp_cfg" "$remote_cfg"

echo "Starting remote docker"
ssh $user@"$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose start $bot_name &"

./logs.sh $bot_name
