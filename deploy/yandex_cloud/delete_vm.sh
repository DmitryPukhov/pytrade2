#!/bin/bash
echo "Stopping biml vm"
yc compute instance stop --name biml-trade-bots || true
echo "Deleting biml vm"
yc compute instance delete --name biml-trade-bots