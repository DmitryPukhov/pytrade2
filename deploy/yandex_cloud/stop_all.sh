#!/bin/bash


user="yc-user"
public_ip="$(yc compute instance list | grep pytrade2-trade-bots | awk '{print $10}')"
pytrade2_vm_dir="/home/$user/pytrade2"

echo "Stopping bots at $public_ip machine"
ssh $user@"$public_ip" "cd $pytrade2_vm_dir ; sudo docker-compose down"

