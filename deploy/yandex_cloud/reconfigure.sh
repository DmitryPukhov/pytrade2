#!/bin/bash

bot_name=${1?"Please provide bot name. Example: $0 lstmstrategy"} # arg1 is a bot name

local_cfg_dir="$(pwd)/secret"
local_cfg="$local_cfg_dir/$bot_name.yaml"
local_log_cfg="$local_cfg_dir/log.cfg"
tmp_cfg_dir="$(pwd)/tmp/pytrade2/pytrade2/cfg"
tmp_cfg="$tmp_cfg_dir/$bot_name.yaml"
tmp_log_cfg="$tmp_cfg_dir/log.cfg"


user="yc-user"
public_ip="$(yc compute instance list | grep pytrade2 | awk '{print $10}')"
pytrade2_vm_dir="/home/$user/pytrade2"
remote_cfg="$user@$public_ip:$pytrade2_vm_dir/pytrade2/cfg/$bot_name.yaml"
remote_log_cfg="$user@$public_ip:$pytrade2_vm_dir/pytrade2/cfg/log.cfg"


echo "Reconfiguring $bot_name at $public_ip machine. Copy $local_cfg to $remote_cfg"

echo "Stopping remote docker"
ssh "$user@$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose stop $bot_name"

echo "Copy new configs to remote"
cp -f "$local_cfg" "$tmp_cfg"
scp "$tmp_cfg" "$remote_cfg"

echo "Copy new log cfg to remote"
cp -f "$local_log_cfg" "$tmp_log_cfg"
scp "$tmp_log_cfg" "$remote_log_cfg"


echo "Starting remote docker"
ssh $user@"$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose start $bot_name &"

./logs.sh $bot_name
