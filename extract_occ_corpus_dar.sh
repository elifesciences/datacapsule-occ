#!/bin/bash

set -e

TEMP_DIR=.temp

if [ -f .config ]; then
  source .config
fi

function realpath { echo $(cd $(dirname "$1"); pwd)/$(basename "$1"); }

original_pwd=$(pwd)

for filename in $TEMP_DIR/*/*.1.dar; do
  abs_filename=$(realpath "$filename")
  abs_dar_basename=${filename%.1.dar}
  echo $abs_dar_basename
  target_dir=$(dirname "$filename")
  cd "$target_dir"
  dar -x $abs_dar_basename --comparison-field=mtime
  cd "$original_pwd"
done
