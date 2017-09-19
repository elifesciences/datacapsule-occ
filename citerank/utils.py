import os
import errno
import csv
from multiprocessing import Pool
from contextlib import contextmanager

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
  import six

  if mode == 'w' and not six.PY2:
    from io import TextIOWrapper

    return TextIOWrapper(gzip.open(filename, mode))
  else:
    return gzip.open(filename, mode)
