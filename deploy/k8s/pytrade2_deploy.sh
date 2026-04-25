#!/bin/bash

echo "Deploying PyTrade2 to k8s..."
# Docker build, docker push to image repository
./pytrade2_build_push.sh

# Install pytrade2 helm chart
./pytrade2_install_chart.sh