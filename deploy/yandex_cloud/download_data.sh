#!/bin/bash

public_ip="$(yc compute instance list | grep pytrade2-trade-bots | awk '{print $10}')"
user="yc-user"
local_pytrade2_dir="$(pwd)/../.."
local_data_dir="$local_pytrade2_dir/data/yandex-cloud"

# Download from remote to local folder
download_data(){
  mkdir -p $local_data_dir
  vm_data_dir="/home/$user/pytrade2/data"
  echo "Downloading bots data from $public_ip to $local_data_dir"
  rsync -v -r $user@$public_ip:$vm_data_dir/  $local_data_dir
}

# Print strategies trades info
print_last_trades(){
  for strategy_dir in $local_data_dir/*
  do
    strategy="$(basename -- $strategy_dir)"
    strategy_file="$strategy_dir/$strategy.db"
    last_close_time=$(sqlite3 "$strategy_file" "SELECT MAX(close_time) from trade")
    last_open_time=$(sqlite3 "$strategy_file" "SELECT MAX(open_time) from trade")
    echo "$strategy last opened: $last_open_time, closed: $last_close_time"
  done
  echo "Current time $(date -u)"
}


download_data
print_last_trades
