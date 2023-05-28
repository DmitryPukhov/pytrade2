#!/bin/bash

#####################################################
# Purge remote data directory, leave only last 2 days
#####################################################

public_ip=$(yc vpc address list | grep pytrade2 | awk '{print $6}')
user="yc-user"
data_dir="/home/$user/pytrade2/data"


echo "Purge data in $public_ip"

ssh "$user@$public_ip" "cd pytrade2/data; \
 for strategy_dir in \$(ls); do \
    strategy_data_dir=$data_dir/\$strategy_dir\/Xy;
    cd \$strategy_data_dir;
    last_file=\$(ls | sort | tail -n 1)
    prefix=\${last_file:0:10}
    echo \"Purging \$strategy_data_dir. Leave only \$prefix \"
    sudo find . ! -name \"\${prefix}*\" -type f -delete
 done"

