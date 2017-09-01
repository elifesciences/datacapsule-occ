import logging

from lxml import etree

from citerank.utils import default_get_request_handler

OAI_SETS_URL = 'http://oai.crossref.org/OAIHandler?verb=ListSets'
OAI_SET_IDS_URL_PREFIX = (
  'http://oai.crossref.org/OAIHandler?verb=ListIdentifiers&metadataPrefix=cr_unixml&set='
)

OAI_NS = 'http://www.openarchives.org/OAI/2.0/'
OAI_NS_PREFIX = '{' + OAI_NS + '}'
OAI_PMH_ROOT_TAG = OAI_NS_PREFIX + 'OAI-PMH'
REQUEST_TAG = OAI_NS_PREFIX + 'request'
LIST_SETS_TAG = OAI_NS_PREFIX + 'ListSets'
SET_TAG = OAI_NS_PREFIX + 'set'
SET_SPEC_TAG = OAI_NS_PREFIX + 'setSpec'
SET_NAME_TAG = OAI_NS_PREFIX + 'setName'

LIST_IDENTIFIERS_TAG = OAI_NS_PREFIX + 'ListIdentifiers'
HEADER_TAG = OAI_NS_PREFIX + 'header'
IDENTIFIER_TAG = OAI_NS_PREFIX + 'identifier'

RESUMPTION_TOKEN_ATTRIB = 'resumptionToken'

SET_PATH = '/'.join([LIST_SETS_TAG, SET_TAG])
LIST_IDENTIFIER_PATH = '/'.join([LIST_IDENTIFIERS_TAG, HEADER_TAG])

def get_logger():
  return logging.getLogger(__name__)

def _get_child_text(parent_element, child_tag):
  child_element = parent_element.find(child_tag)
  return child_element.text if child_element is not None else None

def _add_url_param(url, name, value):
  return '{}{}{}={}'.format(url, '&' if '?' in url else '?', name, value)

def _url_with_resumption_token(url, resumption_token=None):
  return _add_url_param(url, 'resumptionToken', resumption_token) if resumption_token else url

def _parse_response_as_xml(response):
  return etree.fromstring(response.content)

class CrossRefOaiApi(object):
  def __init__(self, get_request_handler=None):
    if get_request_handler is None:
      get_request_handler = default_get_request_handler()
    self.get_request_handler = get_request_handler

  def _iter_oai_list_chunks(self, url):
    current_url = url
    while current_url is not None:
      response = self.get_request_handler(current_url)
      response.raise_for_status()
      xml_root = _parse_response_as_xml(response)
      request_element = xml_root.find(REQUEST_TAG)
      resumption_token = (
        request_element.attrib.get(RESUMPTION_TOKEN_ATTRIB)
        if request_element is not None
        else None
      )
      yield xml_root
      if resumption_token:
        current_url = _url_with_resumption_token(url, resumption_token=resumption_token)
      else:
        current_url = None

  def iter_sets(self):
    for response_xml in self._iter_oai_list_chunks(OAI_SETS_URL):
      for element in response_xml.findall(SET_PATH):
        yield {
          'set_id': _get_child_text(element, SET_SPEC_TAG),
          'name': _get_child_text(element, SET_NAME_TAG)
        }

  def iter_set_ids(self):
    for set_result in self.iter_sets():
      yield set_result['set_id']

  def iter_work_ids_in_set(self, set_id):
    for response_xml in self._iter_oai_list_chunks(OAI_SET_IDS_URL_PREFIX + set_id):
      for element in response_xml.findall(LIST_IDENTIFIER_PATH):
        yield _get_child_text(element, IDENTIFIER_TAG)
