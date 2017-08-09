#!/bin/bash

TEMP_DIR=.temp

if [ -f .config ]; then
  source .config
fi

for filename in $TEMP_DIR/*.zip; do
  echo $filename
  target_dir=${filename%.*}
  if [ -d $target_dir ]; then
    rm -rf $target_dir
  fi
  unzip $filename -d $target_dir
done
