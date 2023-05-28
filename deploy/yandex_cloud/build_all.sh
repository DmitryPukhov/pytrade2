#!/bin/bash

public_ip="$(yc compute instance list | grep pytrade2 | awk '{print $10}')"
user="yc-user"
tmp_dir="$(pwd)/tmp"
pytrade2_dir="$(pwd)/../.."

prepare_tmp(){
  echo "Cleaning tmp dir: $tmp_dir"
  rm -r -f "$tmp_dir"
  mkdir -p "$tmp_dir/pytrade2"

  echo "Copying pytrade2 to tmp"
  cp -r "$pytrade2_dir/pytrade2" "$tmp_dir/pytrade2"

  echo "Preparing pytrade2 config"
  rm "$tmp_dir/pytrade2/pytrade2/cfg/app-dev.yaml"
  files=$(ls ./secret/*.yaml)
  for f in $files; do cp -v -f $f ./tmp/pytrade2/pytrade2/cfg/; done

  echo "Copying docker files"
  for file in  "Dockerfile" "requirements.txt" "docker-compose.yml"
  do
    cp "$pytrade2_dir/$file" "$tmp_dir/pytrade2/"
  done
}

copy_to_remote() {
  echo "Copy pytrade2 to $public_ip"
  rsync -v -r "$tmp_dir/pytrade2" $user@"$public_ip":/home/$user/
}

build_docker() {
  echo "Building pytrade2 at $public_ip"
  #ssh $user@"$public_ip" "cd /home/$user/pytrade2 ; sudo docker-compose build lstm simplekeras"
  ssh $user@"$public_ip" "cd /home/$user/pytrade2 ; sudo docker-compose build"
}

build_baremetal() {
    # In case of running without docker
    ssh $user@"$public_ip" "sudo apt install -y pip"
    ssh $user@"$public_ip" "cd /home/$user/pytrade2 ; sudo pip install -r requirements.txt"
}

###### Main
prepare_tmp
copy_to_remote
build_docker

