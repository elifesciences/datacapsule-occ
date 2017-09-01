from __future__ import absolute_import

import argparse
import logging
import os
import csv

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
    '--output-path', type=str, required=False,
    default='.temp',
    help='path to download directory'
  )
  parser.add_argument(
    '--beam', action='store_true',
    help='use apache beam'
  )
  parser.add_argument(
    '--cloud', action='store_true',
    help='use cloud (implies "beam")'
  )
  parser.add_argument(
    '--runner',
    required=False,
    help='Apache Beam runner (implies "beam").'
  )
  parser.add_argument(
    '--processes',
    type=int,
    default=100,
    help='Number of separate download processes.'
  )
  parser.add_argument(
    '--max-retries',
    type=int,
    default=3,
    help='Number of HTTP request retries.'
  )
  return parser

def download_sets_direct(csv_filename, oai_api):
  from tqdm import tqdm

  if os.path.isfile(csv_filename):
    os.remove(csv_filename)
  temp_filename = csv_filename + '.part'
  with open(temp_filename, 'w') as csv_f:
    csv_writer = csv.writer(csv_f)
    csv_writer.writerow([
      'set_id',
      'name'
    ])
    with tqdm(oai_api.iter_sets(), leave=False) as pbar:
      for set_obj in pbar:
        pbar.set_description('{:30.30s}'.format(set_obj['set_id']))
        csv_writer.writerow([
          set_obj['set_id'],
          set_obj['name'].encode('utf8')
        ])
  os.rename(temp_filename, csv_filename)

def download_work_ids_for_set_direct(csv_filename, oai_api, set_id, zipped=True):
  from tqdm import tqdm

  if zipped:
    from citerank.utils import gzip_open

    csv_filename += '.gz'
    stream_open = gzip_open
  else:
    stream_open = open

  if os.path.isfile(csv_filename):
    os.remove(csv_filename)
  temp_filename = csv_filename + '.part'
  with stream_open(temp_filename, 'w') as csv_f:
    csv_writer = csv.writer(csv_f)
    csv_writer.writerow([
      'set_id',
      'work_id'
    ])
    with tqdm(oai_api.iter_work_ids_in_set(set_id), leave=False, disable=True) as pbar:
      for work_id in oai_api.iter_work_ids_in_set(set_id):
        pbar.set_description(work_id)
        csv_writer.writerow([
          set_id,
          work_id
        ])
  os.rename(temp_filename, csv_filename)

def download_work_ids_for_set_direct_if_not_exists(set_id, output_path, max_retries):
  from requests.exceptions import HTTPError
  from citerank.utils import get_request_handler_with_retry

  logger = get_logger()

  try:
    work_ids_csv_output_path = os.path.join(output_path, 'work-ids-{}.csv'.format(set_id))
    if not os.path.isfile(work_ids_csv_output_path):
      logger.debug('retrieving work ids for %s', set_id)

      oai_api = CrossRefOaiApi(get_request_handler_with_retry(max_retries=max_retries))
      download_work_ids_for_set_direct(work_ids_csv_output_path, oai_api, set_id)
      logger.debug('done retrieving work ids for %s', set_id)
      return 1
    else:
      logger.debug('work ids already retrieved for %s', set_id)
      return 0
  except HTTPError as e:
    logger.warning('error retrieving work ids for %s due to %s', set_id, e)
    return -1

def download_work_ids_for_sets_direct_if_not_exists(set_ids, output_path, processes=None, **kwargs):
  from functools import partial
  from collections import Counter
  from citerank.utils import tqdm_multiprocessing_map

  process_set_id = partial(
    download_work_ids_for_set_direct_if_not_exists,
    output_path=output_path,
    **kwargs
  )
  results = Counter(tqdm_multiprocessing_map(process_set_id, set_ids, processes=processes))
  get_logger().info(
    'retrieved work ids: success: %s, skipped: %s, error: %s',
    results[1],
    results[0],
    results[-1]
  )


def download_direct(argv):
  from citerank.utils import makedirs, iter_read_csv_columns

  logger = get_logger()

  args = get_args_parser().parse_args(argv)

  output_path = args.output_path
  makedirs(output_path, exist_ok=True)

  sets_filename = os.path.join(output_path, 'sets.csv')

  get_request_handler = None

  oai_api = CrossRefOaiApi(get_request_handler=get_request_handler)

  if not os.path.isfile(sets_filename):
    logger.info('retrieving sets')
    download_sets_direct(sets_filename, oai_api)
    logger.info('done retrieving sets')
  else:
    logger.info('sets already retrieved')

  work_ids_output_path = os.path.join(output_path, 'work-ids')
  makedirs(work_ids_output_path, exist_ok=True)

  download_work_ids_for_sets_direct_if_not_exists(
    [row[0] for row in iter_read_csv_columns(sets_filename, ['set_id'])],
    output_path=work_ids_output_path,
    processes=args.processes,
    max_retries=args.max_retries
  )

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
  from citerank.utils import get_request_handler_with_retry

  args, pipeline_args = get_args_parser().parse_known_args(argv)

  output_path = args.output_path
  work_ids_csv_output_path = os.path.join(output_path, 'work-ids.csv')

  pipeline_options = PipelineOptions(pipeline_args)

  runner = args.runner

  if not runner:
    runner = 'DataflowRunner' if args.cloud else 'FnApiRunner'

  if runner == 'FnApiRunner':
    runner = create_fn_api_runner()

  get_request_handler = get_request_handler_with_retry(max_retries=args.max_retries)
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
    output |= "Format Row" >> beam.Map(lambda element: ','.join(list(element)))
    output |= "Write Output" >> WorkaroundWriteToText(
      work_ids_csv_output_path,
      header='set_id,work_id',
      append_uid=(runner == 'FnApiRunner')
    )

    # Execute the pipeline and wait until it is completed.

def main(argv=None):
  args, _ = get_args_parser().parse_known_args(argv)

  if args.beam or args.cloud or args.runner:
    download_using_apache_beam(argv)
  else:
    download_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
