import os
import errno
import csv
from multiprocessing import Pool
from contextlib import contextmanager

import six

TEMP_FILE_SUFFIX = '.part'

def makedirs(path, exist_ok=False):
  try:
    # Python 3
    os.makedirs(path, exist_ok=exist_ok)
  except TypeError:
    # Python 2
    try:
      os.makedirs(path)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise

def iter_read_csv_columns(filename, columns):
  with open(filename, 'r') as csv_f:
    csv_reader = csv.reader(csv_f)
    first_row = True
    indices = None
    for row in csv_reader:
      if first_row:
        indices = [row.index(c) for c in columns]
        first_row = False
      else:
        yield [row[i] for i in indices]

@contextmanager
def terminating(thing):
  try:
    yield thing
  finally:
    thing.terminate()

def create_pool(**kwargs):
  return terminating(Pool(**kwargs))

def tqdm_multiprocessing_map(f, iterable, processes=None):
  from tqdm import tqdm

  with create_pool(processes=processes) as p:
    with tqdm(total=len(iterable)) as pbar:
      result = []
      imap_result = p.imap_unordered(f, iterable)
      for x in imap_result:
        pbar.update()
        result.append(x)
      return result

def default_get_request_handler():
  from requests import get as requests_get
  return requests_get

def configure_session_retry(
  session=None, max_retries=3, backoff_factor=1, status_forcelist=None,
  **kwargs):

  import requests
  from requests.packages.urllib3 import Retry

  retry = Retry(
    connect=max_retries,
    read=max_retries,
    status_forcelist=status_forcelist,
    redirect=5,
    backoff_factor=backoff_factor
  )
  session.mount('http://', requests.adapters.HTTPAdapter(max_retries=retry, **kwargs))
  session.mount('https://', requests.adapters.HTTPAdapter(max_retries=retry, **kwargs))

def get_request_handler_with_retry(max_retries=3, backoff_factor=1, status_forcelist=None):
  import requests

  if status_forcelist is None:
    status_forcelist = [500, 503]
  def get_request_handler(url, **kwargs):
    s = requests.Session()
    configure_session_retry(
      session=s,
      max_retries=max_retries,
      backoff_factor=backoff_factor,
      status_forcelist=status_forcelist
    )
    return s.get(url, **kwargs)
  return get_request_handler

def gzip_open(filename, mode):
  import gzip

  if mode == 'w' and not six.PY2:
    from io import TextIOWrapper

    return TextIOWrapper(gzip.open(filename, mode))
  else:
    return gzip.open(filename, mode)

def optionally_compressed_open(filename, mode):
  if filename.endswith('.gz') or filename.endswith('.gz' + TEMP_FILE_SUFFIX):
    return gzip_open(filename, mode)
  else:
    return open(filename, mode)

def open_csv_output(filename):
  return optionally_compressed_open(filename, 'w')

def write_csv_rows(writer, iterable):
  if six.PY2:
    for row in iterable:
      writer.writerow([
        x.encode('utf-8') if isinstance(x, six.text_type) else x
        for x in row
      ])
  else:
    for row in iterable:
      writer.writerow(row)

def write_csv_row(writer, row):
  write_csv_rows(writer, [row])

def write_csv(filename, columns, iterable):
  temp_filename = filename + TEMP_FILE_SUFFIX
  if os.path.isfile(filename):
    os.remove(filename)
  with open_csv_output(temp_filename) as csv_f:
    writer = csv.writer(csv_f)
    write_csv_rows(writer, [columns])
    write_csv_rows(writer, iterable)
  os.rename(temp_filename, filename)
