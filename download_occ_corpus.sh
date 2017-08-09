#!/bin/bash

TEMP_DIR=.temp

if [ -f .config ]; then
  source .config
fi

python -m citerank.download_occ_corpus --download-path $TEMP_DIR $@
