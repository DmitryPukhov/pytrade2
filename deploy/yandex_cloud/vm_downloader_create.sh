#!/bin/bash
app_name=pytrade2-streamdownloader
zone="ru-central1-a"

create_instance() {
  echo "Creating virtual machine $app_name"
  # standard-v2 cascade lake or standard-v3 ice lake
  yc compute instance create \
    --name $app_name \
    --hostname $app_name \
    --zone $zone --memory 2 \
    --cores 2 \
    --platform "standard-v3" \
    --network-interface subnet-name=default-ru-central1-a,nat-ip-version=ipv4 \
    --create-boot-disk image-folder-id=standard-images,image-family=ubuntu-2204-lts,type=network-hdd,size=64 \
    --ssh-key ./secret/id_rsa.pub
}

install_prereq() {

  public_ip=$(yc compute instance list --format json | jq -r '.[] | select(.name == "pytrade2-streamdownloader") | .network_interfaces[0].primary_v4_address.one_to_one_nat.address')
  user="yc-user"
  echo "Found public ip: $public_ip for vm $app_name"
  echo "Install prerequisites to $public_ip"

  # Remove old instance references
  ssh-keygen -y -f "/home/dima/.ssh/known_hosts" -R "$public_ip"

  ssh "$user@$public_ip" "sudo add-apt-repository -y ppa:deadsnakes/ppa && sudo apt update && sudo apt install -y python3.11 & sudo apt install -y snapd"
  #ssh "$user@$public_ip" "sudo snap install yes"
  ssh "$user@$public_ip" "sudo snap refresh snapd"
  ssh "$user@$public_ip" "sudo snap install docker"
}

# Main

# Vm
#create_instance

# Docker
install_prereq
