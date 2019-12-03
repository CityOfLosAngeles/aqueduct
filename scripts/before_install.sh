#!/bin/bash

# Remove the previous repository so we don't get conflicts
# on trying to copy the new files during deployment.
rm -rf /home/ec2-user/aqueduct
