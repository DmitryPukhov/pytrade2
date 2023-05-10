#!/bin/bash

public_ip="$(yc compute instance list | grep biml-trade-bots | awk '{print $10}')"
user="yc-user"
local_biml_dir="$(pwd)/../.."
local_data_dir="$local_biml_dir/data/yandex-cloud"
mkdir -p $local_data_dir
vm_data_dir="/home/$user/biml/data"
echo "Downloading bots data from $public_ip to $local_data_dir"
cmd="rsync -v -r $user@$public_ip:$vm_data_dir/  $local_data_dir"
echo $cmd
$cmd
