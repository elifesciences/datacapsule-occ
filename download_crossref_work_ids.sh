#!/bin/bash

set -e

TEMP_DIR=.temp

if [ -f .config ]; then
  source .config
fi

python -m citerank.crossref.download_work_ids --work-ids-csv-output-path $TEMP_DIR/work-ids.csv $@
