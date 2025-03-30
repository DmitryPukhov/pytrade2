#!/bin/bash

. deploy_lib.sh
export VM_PUBLIC_IP="$(yc compute instance list | grep pytrade2-streamdownloader | awk '{print $10}')"
echo "VM_PUBLIC_IP=$VM_PUBLIC_IP"
bot_names="streamdownloader"

update_tmp(){
  echo "Update tmp dir ${TMP_DIR}"
  # copy env file to local tmp folder
  cp -r "$PYTRADE2_DIR/yandex-cloud-streamdownloader.env" "$TMP_DIR/pytrade2/.env"
  #cp -r "$PYTRADE2_DIR/pyproject-downloader.toml" "$TMP_DIR/pytrade2/pyproject.toml"
}

#### Main
echo "Deploying $bot_names"
dockers_down "$bot_names"

prepare_tmp
update_tmp


copy_to_remote
build_docker

dockers_up "$bot_names"
