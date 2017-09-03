from __future__ import absolute_import

import argparse
import logging
import os
import re
import json
from zipfile import ZipFile
from io import BufferedReader
from urllib.parse import quote
from requests_futures.sessions import FuturesSession

from citerank.utils import configure_session_retry


def get_logger():
  return logging.getLogger(__name__)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Download Crossref Works data'
  )
  parser.add_argument(
    '--output-path', type=str, required=False,
    default='.temp',
    help='path to download directory'
  )
  parser.add_argument(
    '--max-retries',
    type=int,
    default=10,
    help='Number of HTTP request retries.'
  )
  parser.add_argument(
    '--batch-size',
    type=int,
    default=1000,
    help='Number rows per page to retrieve.'
  )
  return parser

def iter_page_responses(base_url, max_retries, start_cursor='*'):
  logger = get_logger()

  next_cursor_pattern = re.compile(r'"next-cursor":"([^"]+?)"')

  with FuturesSession(max_workers=10) as session:
    configure_session_retry(
      session=session,
      max_retries=max_retries
    )

    def request_page(cursor):
      url = '{}{}cursor={}'.format(
        base_url, '&' if '?' in base_url else '?', quote(cursor)
      )
      return session.get(url, stream=True)

    future_response = request_page(start_cursor)
    while future_response:
      response = future_response.result()
      response.raw.decode_content = True

      # try to find the next cursor in the first response characters
      # we don't need to wait until the whole response has been received
      raw = BufferedReader(response.raw)
      first_chars = raw.peek(1000).decode()
      m = next_cursor_pattern.search(first_chars)
      next_cursor = m.group(1).replace('\\/', '/') if m else None
      logger.info('next_cursor: %s', next_cursor)

      if next_cursor:
        # request the next page as soon as possible,
        # we will read the result in the next iteration
        future_response = request_page(next_cursor)
      else:
        future_response = None

      content = raw.read()
      yield next_cursor, content

def save_page_responses(base_url, zip_filename, max_retries, items_per_page):
  logger = get_logger()

  state_filename = zip_filename + '.meta'
  page_filename_pattern = '{}-page-{{}}-offset-{{}}.json'.format(
    os.path.splitext(os.path.basename(zip_filename))[0]
  )

  start_cursor = '*'
  offset = 0
  page_index = 0
  if os.path.isfile(state_filename):
    with open(state_filename, 'r') as meta_f:
      previous_state = json.load(meta_f)
      start_cursor = previous_state['cursor']
      page_index = previous_state['page_index']
      offset = previous_state['offset']
      if previous_state['items_per_page'] != items_per_page:
        raise RuntimeError('please continue using the same items per page: {}'.format(
          previous_state['items_per_page']
        ))

  logger.info('start cursor: %s (offset %s)', start_cursor, offset)

  with ZipFile(zip_filename, 'a') as zf:
    page_responses = iter_page_responses(
      base_url,
      max_retries=max_retries,
      start_cursor=start_cursor
    )

    for next_cursor, page_response in page_responses:
      logger.info('response: %s (%s)', len(page_response), next_cursor)

      zf.writestr(page_filename_pattern.format(page_index, offset), page_response)

      page_index += 1
      offset += items_per_page

      if next_cursor:
        state_str = json.dumps({
          'cursor': next_cursor,
          'offset': offset,
          'page_index': page_index,
          'items_per_page': items_per_page
        })
        with open(state_filename, 'w') as meta_f:
          meta_f.write(state_str)

def download_works_direct(zip_filename, batch_size, max_retries):
  save_page_responses(
    'http://api.crossref.org/works?rows={}'.format(
      batch_size
    ),
    zip_filename=zip_filename,
    max_retries=max_retries,
    items_per_page=batch_size
  )

def download_direct(argv):
  from citerank.utils import makedirs

  args = get_args_parser().parse_args(argv)

  output_path = args.output_path
  makedirs(output_path, exist_ok=True)

  json_filename_pattern = os.path.join(output_path, 'works.zip')

  download_works_direct(
    json_filename_pattern,
    batch_size=args.batch_size,
    max_retries=args.max_retries
  )

def main(argv=None):
  download_direct(argv)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
