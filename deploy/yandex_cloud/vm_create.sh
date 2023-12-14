#!/bin/bash
app_name=pytrade2
zone="ru-central1-a"

create_instance() {
  echo "Creating virtual machine $app_name"
  # standard-v2 cascade lake or standard-v3 ice lake
  yc compute instance create \
    --name $app_name \
    --hostname $app_name \
    --zone $zone --memory 4 \
    --cores 2 \
    --platform "standard-v3" \
    --network-interface subnet-name=default-ru-central1-a,nat-ip-version=ipv4,nat-address="$public_ip" \
    --create-boot-disk image-folder-id=standard-images,image-family=ubuntu-2204-lts,type=network-hdd,size=32 \
    --ssh-key ./secret/id_rsa.pub
}

install_prereq() {
  user="yc-user"
  echo "Install prerequisites to $public_ip"
  ssh "$user@$public_ip" "sudo add-apt-repository -y ppa:deadsnakes/ppa && sudo apt update && sudo apt install -y python3.11"
  ssh "$user@$public_ip" "sudo snap install yes"
  ssh "$user@$public_ip" "sudo snap refresh snapd"
  ssh "$user@$public_ip" "sudo snap install docker"
}

# Main

# Static ip
if [[ "$*" == *"--new-public-ip"* ]]; then
  echo "Creating new public ip"
  yc vpc address create --name $app_name --external-ipv4 zone=$zone
fi

public_ip=$(yc vpc address list | grep $app_name | awk '{print $6}')


# Vm
create_instance

# Docker
install_prereq
