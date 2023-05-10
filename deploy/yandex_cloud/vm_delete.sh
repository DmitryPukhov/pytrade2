#!/bin/bash
vm_name=biml-trade-bots
echo "Stopping biml vm $vm_name"
yc compute instance stop --name $vm_name || true
echo "Deleting biml vm"
yc compute instance delete --name $vm_name