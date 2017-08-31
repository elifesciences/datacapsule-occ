#!/bin/bash

# Use this file by running:
# source prepare-shell.sh [--cloud]

set -e

TEMP_DIR=.temp
SUB_PROJECT_NAME=citerank
DATASET_SUFFIX=

if [ -f .config ]; then
  source .config
fi

USE_CLOUD=false

if [ "$1" == "--cloud" ]; then
  USE_CLOUD=true
fi

if [ $USE_CLOUD == true ]; then
  PROJECT=$(gcloud config list project --format "value(core.project)")
  BUCKET="gs://${PROJECT}-ml"
  GCS_SUB_PROJECT_PATH="${BUCKET}/${SUB_PROJECT_NAME}"
  GCS_PATH="${GCS_SUB_PROJECT_PATH}"
  GCS_DATA_PATH="${GCS_PATH}/data${DATASET_SUFFIX}"
  DATA_PATH=$GCS_DATA_PATH
else
  DATA_PATH=$TEMP_DIR
fi
