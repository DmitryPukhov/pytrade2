#!/bin/bash

bot_name=$1
user="yc-user"
public_ip="$(yc compute instance list | grep pytrade2-trade-bots | awk '{print $10}')"

# Ssh to cloud and follow logs
ssh $user@"$public_ip" "cd /home/$user/pytrade2/ ; sudo docker-compose logs -n 100 --follow $bot_name &"
