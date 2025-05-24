#!/bin/bash

. deploy_lib.sh
export VM_PUBLIC_IP="$(yc compute instance list | grep pytrade2-streamdownloader | awk '{print $10}')"
bot_names="streamdownloader"
./logs.sh $bot_names

