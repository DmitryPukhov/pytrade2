#!/bin/bash

. deploy_lib.sh
local_data_dir="$PYTRADE2_DIR/data/yandex-cloud"

# Sync from s3 to local folder
download_data_s3(){
  mkdir -p "$local_data_dir"
  echo "Sync pytrade2 data from $S3_DATA_URL to $local_data_dir"
  s3cmd sync -v "$S3_DATA_URL/" "$local_data_dir/"
}


# Download from remote virtual machine to local folder
download_data_vm(){
  mkdir -p $local_data_dir
  vm_data_dir="/home/$VM_USER/pytrade2/data"
  echo "Downloading pytrade2 data from $VM_PUBLIC_IP to $local_data_dir"
  rsync -v -r "$VM_USER@$VM_PUBLIC_IP:$vm_data_dir/"  "$local_data_dir"
}

# Print strategies trades info
print_last_trades(){
  for strategy_dir in "$local_data_dir"/*
  do
    strategy="$(basename -- $strategy_dir)"
    strategy_file="$strategy_dir/$strategy.db"
    last_close_time=$(sqlite3 "$strategy_file" "SELECT MAX(close_time) from trade")
    last_open_time=$(sqlite3 "$strategy_file" "SELECT MAX(open_time) from trade")
    echo "$strategy last opened: $last_open_time, closed: $last_close_time"
  done
  echo "Current time $(date -u)"
}


download_data_s3
print_last_trades
