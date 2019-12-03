#!/bin/bash

# Remove the previous repository so we don't get conflicts
# on trying to copy the new files during deployment.
if [ -d /home/ec2-user/aqueduct ]; then
    rm -rf /home/ec2-user/aqueduct
fi
mkdir -vp /home/ec2-user/aqueduct
