#!/bin/bash
app_name=pytrade2-streamdownloader

echo "Stopping vm $app_name"
yc compute instance stop --name $app_name || true

echo "Deleting pytrade2 vm $app_name"
yc compute instance delete --name $app_name


