#!/bin/bash

default_bot_names="lstmstrategy2 lstmstrategy simplekerasstrategy"
bot_names=${*:-"$default_bot_names"}

user="yc-user"
public_ip="$(yc compute instance list | grep pytrade2 | awk '{print $10}')"


local_cfg_dir="$(pwd)/secret"
tmp_cfg_dir="$(pwd)/tmp/pytrade2/pytrade2/cfg"
user="yc-user"
pytrade2_vm_dir="/home/$user/pytrade2"
remote_cfg_dir="$user@$public_ip:$pytrade2_vm_dir/pytrade2/cfg"



echo "Reconfiguring $bot_name at $public_ip machine. Copy $local_cfg to $remote_cfg"


copy_configs() {
  echo "Copy new configs to tmp"
  for f in "$local_cfg_dir"/*.yaml; do cp -fv "$f" "$tmp_cfg_dir"; done
  for f in "$local_cfg_dir"/*.cfg; do cp -fv "$f" "$tmp_cfg_dir"; done
  echo "Copy $tmp_cfg_dir to $remote_cfg_dir"
  rsync -vr "$tmp_cfg_dir/" "$remote_cfg_dir/"
}


####### main #######
echo "Stopping remote dockers"
ssh "$user@$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose stop $bot_names"

copy_configs

echo "Starting remote dockers"
ssh $user@"$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose start $bot_names &"

## Follow logs
ssh $user@"$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose logs -f"
