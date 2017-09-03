#!/bin/bash

set -e

source prepare-shell.sh

RUN_ARGS=(
  --output-path $DATA_PATH
  $@
)

python -m citerank.crossref.download_works ${RUN_ARGS[@]}
