WORK_URL_PREFIX = 'http://api.crossref.org/works/'

class Work(object):
  def __init__(self, work_obj):
    self.work_obj = work_obj.get('message', work_obj)

  def _join_if_list(self, x, sep=' '):
    if isinstance(x, list):
      return sep.join(x)
    return x

  def get_title(self):
    return self._join_if_list(self.work_obj.get('title'))

  def _get_reference_doi(self, reference):
    return reference.get('DOI')

  def get_reference_dois(self):
    references = self.work_obj.get('reference')
    if references:
      return [
        doi
        for doi in [self._get_reference_doi(r) for r in references]
        if doi
      ]
    else:
      return []

class CrossRefRestApi(object):
  def __init__(self, get_request_handler=None):
    if get_request_handler is None:
      from requests import get as requests_get
      get_request_handler = requests_get
    self.get_request_handler = get_request_handler

  def _get_json(self, url):
    response = self.get_request_handler(url)
    response.raise_for_status()
    return response.json()

  def get_raw_work(self, work_id):
    return self._get_json(WORK_URL_PREFIX + work_id)

  def get_work(self, work_id):
    return Work(self._get_json(WORK_URL_PREFIX + work_id))
