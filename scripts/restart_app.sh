#!/bin/bash

sudo systemctl restart aqueduct-worker

if [ "$APPLICATION_NAME" == "Aqueduct-Scheduler" ]; then
  echo "Restarting scheduler and webservers"
  sudo systemctl restart aqueduct-flower
  sudo systemctl restart aqueduct-scheduler
  sudo systemctl restart aqueduct-webserver
fi
