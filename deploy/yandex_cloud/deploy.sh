#!/bin/bash

. deploy_lib.sh
bot_names=${*:-"$DEFAULT_BOT_NAMES"}


#### Main
echo "Deploying $bot_names"
dockers_down "$bot_names"
./build_all.sh
dockers_up "$bot_names"
