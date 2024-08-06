#!/bin/bash
app_name=pytrade2
zone="ru-central1-a"
public_ip=""

create_instance() {
  echo "Creating virtual machine $app_name with public ip: $public_ip"
  # standard-v2 cascade lake or standard-v3 ice lake
  yc compute instance create \
    --name $app_name \
    --hostname $app_name \
    --zone $zone --memory 4 \
    --cores 2 \
    --platform "standard-v3" \
    --network-interface subnet-name=default-ru-central1-a,nat-ip-version=ipv4,nat-address="$public_ip" \
    --create-boot-disk image-folder-id=standard-images,image-family=ubuntu-2204-lts,type=network-hdd,size=96 \
    --ssh-key ./secret/id_rsa.pub
}

install_prereq() {
  user="yc-user"
  echo "Install prerequisites to $public_ip"

  # Remove old instance references
  ssh-keygen -y -f "/home/dima/.ssh/known_hosts" -R "$public_ip"

  ssh "$user@$public_ip" "sudo add-apt-repository -y ppa:deadsnakes/ppa && sudo apt update && sudo apt install -y python3.11"
  ssh "$user@$public_ip" "sudo snap install yes"
  ssh "$user@$public_ip" "sudo snap refresh snapd"
  ssh "$user@$public_ip" "sudo snap install docker"
}

ensure_public_ip() {
  public_ip=$(yc vpc address list | grep $app_name | awk '{print $6}')
  if [ -z "$public_ip" ]; then
    echo "Public ip of $app_name not found. Creating a new one in zone $zone"
    #yc vpc address create --name $app_name --external-ipv4 zone=$zone
  fi
  public_ip=$(yc vpc address list | grep $app_name | awk '{print $6}')
}

# Main

# Creaate new public ip if does not exist
ensure_public_ip

# Vm
create_instance

# Docker
install_prereq
