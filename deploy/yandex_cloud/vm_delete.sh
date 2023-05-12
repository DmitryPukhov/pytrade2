#!/bin/bash
app_name=biml-trade-bots

echo "Stopping biml vm $app_name"
yc compute instance stop --name $app_name || true

echo "Deleting biml vm $app_name"
yc compute instance delete --name $app_name

echo "Deleting static ip for $app_name"
yc vpc address delete $app_name