#!/bin/bash

sudo systemctl restart aqueduct-worker

if [[ ! -z "${AQUEDUCT_SCHEDULER}" ]]; then
  echo "Restarting scheduler and webserver"
  sudo systemctl restart aqueduct-scheduler
  sudo systemctl restart aqueduct-webserver
fi
