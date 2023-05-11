#!/bin/bash
vm_name=biml-trade-bots

create_instance() {
  echo "Creating virtual machine $vm_name"
  yc compute instance create \
    --name $vm_name \
    --hostname $vm_name \
    --zone ru-central1-a \
    --memory 6 \
    --cores 2 \
    --network-interface subnet-name=default-ru-central1-a,nat-ip-version=ipv4 \
    --create-boot-disk image-folder-id=standard-images,image-family=ubuntu-2204-lts,size=32 \
    --ssh-key ./secret/id_rsa.pub
}

install_docker() {
  user="yc-user"
  echo "Install prerequisites to $public_ip"
  ssh "$user"@"$public_ip" "sudo snap install python38"
  ssh $user@"$public_ip" "sudo snap install yes"
  ssh "$user"@"$public_ip" "sudo snap install docker"
}

install_baremetal() {
  ssh "$user"@"$public_ip" "sudo snap install python38"
}

# Create instance with docker
create_instance
lpublic_ip="$(yc compute instance list | grep $vm_name | awk '{print $10}')"
install_docker


