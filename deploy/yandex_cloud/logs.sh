#!/bin/bash

bot_name=${1?"Please provide bot name.\n Example: $0 lstm"} # arg1 is a bot name

user="yc-user"
public_ip="$(yc compute instance list | grep biml-trade-bots | awk '{print $10}')"

ssh $user@"$public_ip" "sudo docker logs -n 100 -f biml-$bot_name &"
