from __future__ import absolute_import

import argparse
import logging
import zipfile
from zipfile import ZipFile

from tqdm import tqdm


DEFLATE = "deflate"
BZIP2 = "bzip2"
LZMA = "lzma"

def get_logger():
  return logging.getLogger(__name__)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Re-compresses a zip archive'
  )
  parser.add_argument(
    '--input-file', type=str, required=True,
    default='.temp',
    help='path to input file'
  )
  parser.add_argument(
    '--output-file', type=str, required=True,
    default='.temp',
    help='path to output file'
  )
  parser.add_argument(
    '--compression',
    type=str,
    choices=[DEFLATE, BZIP2, LZMA],
    default=DEFLATE,
    help='Zip compression to use (requires Python 3.3+).'
  )
  return parser

def recompress(source_zip_filename, target_zip_filename, compression):
  if source_zip_filename == target_zip_filename:
    raise RuntimeError("source and target zip file must not be the same")

  with ZipFile(source_zip_filename, 'r') as source_zf:
    names = source_zf.namelist()
    get_logger().debug('names: %s', names)
    with ZipFile(target_zip_filename, 'w', compression) as target_zf:
      for name in tqdm(names, leave=False):
        with source_zf.open(name, 'r') as source_f:
          target_zf.writestr(name, source_f.read())

def main(argv=None):
  args = get_args_parser().parse_args(argv)

  compression = zipfile.ZIP_DEFLATED
  if args.compression == BZIP2:
    compression = zipfile.ZIP_BZIP2
  elif args.compression == LZMA:
    compression = zipfile.ZIP_LZMA

  recompress(args.input_file, args.output_file, compression)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
