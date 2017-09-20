from __future__ import absolute_import

import argparse
import logging
import os
from threading import Thread, Event
from queue import Queue
from functools import partial

import requests
from urllib.parse import quote

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
  parser.add_argument(
    '--max-workers',
    type=int,
    default=50,
    help='Number of workers to use.'
  )
  return parser

def process_request_with_base_url(session, request, base_url):
  url = '{}{}'.format(
    base_url, quote(str(request))
  )
  response = session.get(url)
  response.raise_for_status()
  content = response.content
  return content

def request_worker(request_queue, max_retries, process_request, process_response, exit_event):
  logger = get_logger()
  with requests.Session() as session:
    configure_session_retry(
      session=session,
      max_retries=max_retries,
      pool_connections=1,
      pool_maxsize=1
    )

    while not exit_event.isSet():
      request = request_queue.get()
      if request is None:
        logger.debug('worker done')
        break

      logger.debug('request: %s', request)

      try:
        content = process_request(session, request)
      except RuntimeError:
        logger.exception('request failed')
        content = None

      process_response(request, content)

      request_queue.task_done()
    logger.debug('worker exit')
    process_response(None)

def process_response_to_queue(request, response=None, response_queue=None):
  response_queue.put((request, response))

def queue_iterable_master(iterable, request_queue, num_workers, exit_event):
  for request in iterable:
    if exit_event.isSet():
      return
    request_queue.put(request)
  for _ in range(num_workers):
    request_queue.put(None)

def DaemonThread(*args, **kwargs):
  t = Thread(*args, **kwargs)
  t.daemon = True
  return t

def iter_item_responses_threaded(base_url, max_retries, ids, num_workers=50):
  request_queue = Queue(maxsize=num_workers * 2)
  response_queue = Queue(maxsize=num_workers * 10)

  exit_event = Event()

  worker = partial(
    request_worker,
    request_queue=request_queue,
    max_retries=max_retries,
    process_request=partial(
      process_request_with_base_url,
      base_url=base_url
    ),
    process_response=partial(
      process_response_to_queue,
      response_queue=response_queue
    ),
    exit_event=exit_event
  )

  master = partial(
    queue_iterable_master,
    iterable=ids,
    request_queue=request_queue,
    num_workers=num_workers,
    exit_event=exit_event
  )

  for _ in range(num_workers):
    DaemonThread(target=worker).start()

  DaemonThread(target=master).start()

  running_workers = num_workers
  while running_workers > 0:
    try:
      request, response = response_queue.get()
      if request is None:
        running_workers -= 1
      else:
        yield request, response
    except KeyboardInterrupt:
      get_logger().info('KeyboardInterrupt')
      exit_event.set()
      while not request_queue.empty():
        request_queue.get_nowait()
      return

def save_item_responses(base_url, zip_filename, max_retries, num_workers, compression):
  logger = get_logger()

  logger.info('creating range')
  ids = range(100 * 1000 * 1000)
  logger.info('getting len')
  total = len(ids)
  logger.info('len: %s', total)
  item_responses = iter_item_responses_threaded(
    base_url=base_url,
    max_retries=max_retries,
    num_workers=num_workers,
    ids=ids
  )
  logger.info('iterating responses')
  for item_id, response in tqdm(item_responses, total=total, leave=True):
    # TODO only implemented so far as to establish speed
    logger.debug('received response: %s - %s', item_id, response)

def download_works_direct(zip_filename, max_retries, num_workers, compression, email):
  save_item_responses(
    'http://doi.crossref.org/search/doi?pid={}&format=unixsd&citeid='.format(
      email
    ),
    zip_filename=zip_filename,
    max_retries=max_retries,
    num_workers=num_workers,
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
    num_workers=args.max_workers,
    compression=None,
    email=args.email
  )

def main(argv=None):
  download_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
