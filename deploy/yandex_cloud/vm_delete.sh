#!/bin/bash
app_name=pytrade2

echo "Stopping pytrade2 vm $app_name"
yc compute instance stop --name $app_name || true

echo "Deleting pytrade2 vm $app_name"
yc compute instance delete --name $app_name


if [[ "$*" == *"--delete-public-ip"* ]]; then
  echo "Deleting static ip for $app_name"
  yc vpc address delete $app_name
fi