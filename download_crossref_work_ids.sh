#!/bin/bash

set -e

source prepare-shell.sh

RUN_ARGS=(
  --output-path $DATA_PATH
  $@
)

if [ $USE_CLOUD == true ]; then
  RUN_ARGS=(
    ${RUN_ARGS[@]}
    --setup_file "./setup.py"
    --project "$PROJECT"
    --temp_location "$DATA_PATH/temp"
    --save_main_session
    --cloud
  )
fi

python -m citerank.crossref.download_work_ids ${RUN_ARGS[@]}
