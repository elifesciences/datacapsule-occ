import logging
import json

from six import iteritems


def get_logger():
  return logging.getLogger(__name__)

class MockResponse(object):
  def __init__(self, result, status_code):
    self.content = result
    self.status_code = status_code

  def raise_for_status(self):
    if self.status_code != 200:
      raise RuntimeError('status code: {}'.format(self.status_code))

  def json(self):
    return json.loads(self.content)

  def __str__(self):
    return '{}: {}'.format(self.status_code, self.content)

def _as_get_response(result):
  if result is None:
    response = MockResponse('', 404)
  else:
    response = MockResponse(result, 200)
  return response

def _with_logging(f):
  def wrapped(*args, **kwargs):
    get_logger().debug('args: %s', args)
    response = f(*args, **kwargs)
    get_logger().debug('result: %s', response)
    return response
  return wrapped

def create_get_request_handler(results, default_result=None):
  default_response = _as_get_response(default_result)
  responses = {
    k: _as_get_response(v)
    for k, v in iteritems(results)
  }
  return _with_logging(lambda url: responses.get(url, default_response))
