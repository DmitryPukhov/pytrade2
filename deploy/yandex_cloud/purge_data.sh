#!/bin/bash

#####################################################
# Purge remote data directory, leave only last 2 days
#####################################################

. deploy_lib.sh
data_dir="$VM_PYTRADE2_DIR/data"


echo "Purge data in $VM_PUBLIC_IP"

ssh "$VM_USER@$VM_PUBLIC_IP" "cd pytrade2/data; \
 for strategy_dir in \$(ls); do \
    for subdir in Xy account; do \
      strategy_data_dir=$data_dir/\$strategy_dir\/\$subdir;
      cd \$strategy_data_dir;
      last_file=\$(ls | sort | tail -n 1)
      prefix=\${last_file:0:10}
      echo \"Purging \$strategy_data_dir Leave only \$prefix \"
      sudo find . ! -name \"\${prefix}*\" -type f -delete
    done;
 done"

