from __future__ import absolute_import

import argparse
import logging
import os
import csv

from tqdm import tqdm

from citerank.utils import (
  makedirs
)

from citerank.crossref.crossref_oai_api import (
  CrossRefOaiApi
)

def get_logger():
  return logging.getLogger(__name__)

def parse_args():
  parser = argparse.ArgumentParser(
    description='Download Crossref Citation data'
  )
  parser.add_argument(
    '--work-ids-csv-output-path', type=str, required=False,
    default='.temp',
    help='path to download directory')
  args = parser.parse_args()
  return args

def main():
  logger = get_logger()
  args = parse_args()

  work_ids_csv_output_path = args.work_ids_csv_output_path
  makedirs(os.path.dirname(work_ids_csv_output_path), exist_ok=True)

  get_request_handler = None

  oai_api = CrossRefOaiApi(get_request_handler=get_request_handler)

  with open(work_ids_csv_output_path, 'w') as csv_f:
    csv_writer = csv.writer(csv_f)
    csv_writer.writerow([
      'set_id',
      'work_id'
    ])
    logger.info('retrieving sets')
    for set_id in tqdm(oai_api.iter_set_ids(), leave=False):
      logger.info('processing set: %s', set_id)
      for work_id in oai_api.iter_work_ids_in_set(set_id):
        logger.debug('work id: %s', work_id)
        csv_writer.writerow([
          set_id,
          work_id
        ])

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
