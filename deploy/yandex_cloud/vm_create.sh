#!/bin/bash
app_name=pytrade2
zone="ru-central1-a"

create_instance() {
  echo "Creating virtual machine $app_name"
  # standard-v2 cascade lake or standard-v3 ice lake
  yc compute instance create \
    --name $app_name \
    --hostname $app_name \
    --zone $zone\
    --memory 8 \
    --cores 4 \
    --platform "standard-v3" \
    --network-interface subnet-name=default-ru-central1-a,nat-ip-version=ipv4,nat-address="$public_ip" \
    --create-boot-disk image-folder-id=standard-images,image-family=ubuntu-2204-lts,type=network-ssd,size=32 \
    --ssh-key ./secret/id_rsa.pub
}

install_prereq() {
  user="yc-user"
  echo "Install prerequisites to $public_ip"
  ssh "$user"@"$public_ip" "sudo snap install python38"
  ssh $user@"$public_ip" "sudo snap install yes"
  ssh "$user"@"$public_ip" "sudo snap install docker"
}

install_baremetal() {
  ssh "$user"@"$public_ip" "sudo snap install python38"
}

# Main

# Static ip
yc vpc address create --name $app_name --external-ipv4 zone=$zone
public_ip=$(yc vpc address list | grep $app_name | awk '{print $6}')

# Vm
create_instance

# Docker
install_prereq


