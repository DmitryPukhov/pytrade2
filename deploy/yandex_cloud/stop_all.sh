#!/bin/bash


user="yc-user"
public_ip="$(yc compute instance list | grep biml-trade-bots | awk '{print $10}')"
biml_vm_dir="/home/$user/biml"

echo "Stopping bots at $public_ip machine"
ssh $user@"$public_ip" "cd $biml_vm_dir ; sudo docker-compose down"

