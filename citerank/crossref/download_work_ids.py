from __future__ import absolute_import

import argparse
import logging
import os
import csv

from citerank.utils import (
  makedirs
)

from citerank.crossref.crossref_oai_api import (
  CrossRefOaiApi
)

def get_logger():
  return logging.getLogger(__name__)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Download Crossref Citation data'
  )
  parser.add_argument(
    '--work-ids-csv-output-path', type=str, required=False,
    default='.temp',
    help='path to download directory'
  )
  parser.add_argument(
    '--beam', action='store_true',
    help='use apache beam'
  )
  parser.add_argument(
    '--runner',
    required=False,
    help='Apache Beam runner (implies "beam").'
  )
  return parser

def download_direct(argv):
  from tqdm import tqdm

  logger = get_logger()

  args = get_args_parser().parse_args(argv)

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

def create_fn_api_runner():
  from apache_beam.runners.portability.fn_api_runner import FnApiRunner
  return FnApiRunner()

def Spy(f):
  def SpyWrapper(*args):
    f(*args)
    if len(args) == 1:
      return args[0]
    return args
  return SpyWrapper

def download_using_apache_beam(argv):
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  from citerank.beam_utils.fileio import WorkaroundWriteToText

  args, pipeline_args = get_args_parser().parse_known_args(argv)

  work_ids_csv_output_path = args.work_ids_csv_output_path

  pipeline_options = PipelineOptions(pipeline_args)

  runner = args.runner
  if not runner or runner == 'FnApiRunner':
    runner = create_fn_api_runner()

  get_request_handler = None
  oai_api = CrossRefOaiApi(get_request_handler=get_request_handler)

  get_logger().info('work_ids_csv_output_path: %s', work_ids_csv_output_path)

  def iter_set_it_and_work_id_in_set(set_id):
    for work_id in oai_api.iter_work_ids_in_set(set_id):
      yield set_id, work_id

  with beam.Pipeline(runner, options=pipeline_options) as p:
    output = p | beam.Create(['DUMMY'])
    output |= "Retrieve Sets" >> beam.FlatMap(lambda _: oai_api.iter_set_ids())
    output |= "Log Set Id" >> beam.Map(Spy(
      lambda set_id: get_logger().info('Processing set_id: %s', set_id)
    ))
    output |= "Retrieve Work Ids" >> beam.FlatMap(iter_set_it_and_work_id_in_set)
    output |= "Format Row" >> beam.Map(lambda (set_id, work_id): ','.join([set_id, work_id]))
    output |= "Write Output" >> WorkaroundWriteToText(
      work_ids_csv_output_path,
      header='set_id,work_id',
      append_uid=(runner == 'FnApiRunner')
    )

    # Execute the pipeline and wait until it is completed.

def main(argv=None):
  args, _ = get_args_parser().parse_known_args(argv)

  if args.beam or args.runner:
    download_using_apache_beam(argv)
  else:
    download_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
