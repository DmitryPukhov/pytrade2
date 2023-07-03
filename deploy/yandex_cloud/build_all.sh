#!/bin/bash

. deploy_lib.sh

###### Main
prepare_tmp
copy_to_remote
build_docker

