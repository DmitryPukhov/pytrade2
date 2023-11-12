#!/bin/bash

. deploy_lib.sh
bot_names=${*:-"$DEFAULT_BOT_NAMES"}


#### Main
echo "Deploying $bot_names"
bots_down "$bot_names"
./build_all.sh
bots_up "$bot_names"
