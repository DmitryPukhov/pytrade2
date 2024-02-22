#!/bin/bash
#DEFAULT_BOT_NAMES="longcandledensestrategy lstmbidaskregressionstrategy2 lstmbidaskregressionstrategy kerasbidaskregressionstrategy"

DEFAULT_BOT_NAMES='lgblowhighregressionstrategy'
S3_DATA_URL='s3://pytrade2/data'
VM_PUBLIC_IP="$(yc compute instance list | grep pytrade2 | awk '{print $10}')"
VM_USER="yc-user"
TMP_DIR="$(pwd)/tmp"
PYTRADE2_DIR="$(pwd)/../.."
VM_PYTRADE2_DIR="/home/$VM_USER/pytrade2"

prepare_tmp_config(){
   echo "Preparing pytrade2 config"
   # cfg from pytrade2 project is already there
   rm "$TMP_DIR/pytrade2/pytrade2/cfg/app-dev.yaml"
   rm "$TMP_DIR/pytrade2/pytrade2/cfg/log-dev.yaml"
   tmp_cfg_dir=./tmp/pytrade2/pytrade2/cfg/
   # Copy app.yaml and strategies yamls
   files=$(ls ./cfg/*.yaml)
   for f in $files
   do
     fname=$(basename "$f")
     cp -v -f $f $tmp_cfg_dir
     cat "./secret/$fname" >> "$tmp_cfg_dir/$fname"
    done
   # Copy log.cfg
   files=$(ls ./cfg/*.cfg)
   for f in $files; do cp -v -f $f $tmp_cfg_dir; done
}

prepare_tmp_clean_cache(){
  echo "Clean cache files"
  for name in __pycache__ _trial_temp "*.lock" "MagicMock"
  do
    find "$TMP_DIR" -type d -name "$name" -exec rm -rf {} \; 2> /dev/null # Supress cannot find messages
    find "$TMP_DIR" -name "$name" -exec rm -rf {} \; 2> /dev/null # Supress cannot find messages
  done
}

prepare_tmp(){
  echo "Cleaning tmp dir: $TMP_DIR"
  rm -r -f "$TMP_DIR"
  mkdir -p "$TMP_DIR/pytrade2"

  echo "Copying pytrade2 to tmp"
  cp -r "$PYTRADE2_DIR/pytrade2" "$TMP_DIR/pytrade2"

  # Clean caches
  prepare_tmp_clean_cache
  # Compose configs in tmp
  prepare_tmp_config

  echo "Copying docker files"
  for file in  "Dockerfile" "requirements.txt" "docker-compose.yml"
  do
    cp "$PYTRADE2_DIR/$file" "$TMP_DIR/pytrade2/"
  done
}

copy_to_remote() {
  echo "Copy pytrade2 to $VM_PUBLIC_IP"
  rsync -v -r "$TMP_DIR/pytrade2" $VM_USER@"$VM_PUBLIC_IP":/home/$VM_USER/
}
copy_to_remote_config() {
  echo "Copy pytrade2 config to $VM_PUBLIC_IP"
  rsync -v -r "$TMP_DIR/pytrade2/pytrade2/cfg/" $VM_USER@"$VM_PUBLIC_IP":/home/$VM_USER/pytrade2/pytrade2/cfg
}


build_docker() {
  echo "Building pytrade2 at $VM_PUBLIC_IP"
  #ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker-compose build"
  # build with DOCKER_BUILDKIT=0 to avoid the error: https://github.com/docker/cli/issues/4437
  ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR; sudo DOCKER_BUILDKIT=0 docker-compose build"

}

build_baremetal() {
    # In case of running without docker
    ssh $VM_USER@"$VM_PUBLIC_IP" "sudo apt install -y pip"
    ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo pip install -r requirements.txt"
}

bots_up() {
  bot_names=$1
  echo "Starting bot $bot_names at $VM_PUBLIC_IP machine"
  ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker compose up $bot_names &"
}

bots_down() {
  bot_names=$1
  echo "Stopping bot $bot_names at $VM_PUBLIC_IP machine"
  #ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker compose down $bot_names"
  ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker stop $bot_names"
  ssh $VM_USER@"$VM_PUBLIC_IP" "cd $VM_PYTRADE2_DIR ; sudo docker rm $bot_names"
}