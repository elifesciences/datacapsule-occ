from __future__ import absolute_import

import argparse
import logging
import os
from urllib.parse import quote

from requests_futures.sessions import FuturesSession
from tqdm import tqdm

from citerank.utils import configure_session_retry

def get_logger():
  return logging.getLogger(__name__)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Download Crossref Works By id (Experimental / incomplete)'
  )
  parser.add_argument(
    '--output-file', type=str, required=False,
    help='path to output file'
  )
  parser.add_argument(
    '--max-retries',
    type=int,
    default=10,
    help='Number of HTTP request retries.'
  )
  parser.add_argument(
    '--email',
    type=str,
    required=True,
    help='Crossref email to login with.'
  )
  return parser

def iter_item_responses(base_url, max_retries, ids):
  from threading import Thread
  from queue import Queue
  import requests

  max_workers = 500

  with requests.Session() as session:
    configure_session_retry(
      session=session,
      max_retries=max_retries,
      pool_connections=max_workers,
      pool_maxsize=max_workers
    )

    def request_id(item_id):
      url = '{}{}'.format(
        base_url, quote(str(item_id))
      )
      response = session.get(url)
      response.raise_for_status()
      content = response.content
      return item_id, content

    id_queue = Queue(maxsize=max_workers * 2)
    result_queue = Queue(maxsize=max_workers * 10)

    def id_worker():
      while True:
        item_id = id_queue.get()
        if item_id is None:
          result_queue.put(None)
          break
        get_logger().debug('next id: %s', item_id)
        _, content = request_id(item_id)
        result_queue.put((item_id, content))
        id_queue.task_done()

    def master():
      for item_id in ids:
        id_queue.put(item_id)
      for _ in range(max_workers):
        id_queue.put(None)

    for _ in range(max_workers):
      Thread(target=id_worker).start()

    Thread(target=master).start()

    running_workers = max_workers
    while running_workers > 0:
      result = result_queue.get()
      if result is None:
        running_workers -= 1
      else:
        item_id, content = result
        yield item_id, content

def save_item_responses(base_url, zip_filename, max_retries, compression):
  logger = get_logger()

  get_logger().info('creating range')
  ids = range(100 * 1000 * 1000)
  get_logger().info('getting len')
  total = len(ids)
  get_logger().info('len: %s', total)
  item_responses = iter_item_responses(
    base_url=base_url,
    max_retries=max_retries,
    ids=ids
  )
  get_logger().info('iterating responses')
  for item_id, response in tqdm(item_responses, total=total, leave=True):
    # TODO only implemented so far as to establish speed
    pass

def download_works_direct(zip_filename, max_retries, compression, email):
  save_item_responses(
    'http://doi.crossref.org/search/doi?pid={}&format=unixsd&citeid='.format(
      email
    ),
    zip_filename=zip_filename,
    max_retries=max_retries,
    compression=compression
  )

def download_direct(argv):
  from citerank.utils import makedirs

  args = get_args_parser().parse_args(argv)

  output_file = args.output_file
  if output_file:
    makedirs(os.path.basename(output_file), exist_ok=True)

  download_works_direct(
    output_file,
    max_retries=args.max_retries,
    compression=None,
    email=args.email
  )

def main(argv=None):
  download_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
