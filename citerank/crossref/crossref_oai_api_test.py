from __future__ import absolute_import

import logging

from citerank.crossref.crossref_oai_api import (
  OAI_SETS_URL,
  OAI_SET_IDS_URL_PREFIX,
  CrossRefOaiApi
)

from citerank.crossref.test_utils import (
  create_get_request_handler
)

OAI_PMH_OPEN_TAG =\
'''<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">'''

OAI_PMH_CLOSE_TAG = '</OAI-PMH>'

SET_ID_1 = 'spec:1'
NAME_1 = 'Name 1'

SET_ID_2 = 'spec:2'
NAME_2 = 'Name 2'

RESUMPTION_TOKEN_1 = 'rt1'

OAI_SET_IDS_URL_1 = OAI_SET_IDS_URL_PREFIX + SET_ID_1

WORK_ID_1 = 'work:1'
WORK_ID_2 = 'work:2'

def get_logger():
  return logging.getLogger(__name__)

def setup_module():
  logging.basicConfig(level='DEBUG')


def _format_request_token(resumptionToken):
  return '<request resumptionToken="{}">http://oai.crossref.org/OAIHandler</request>'.format(
    resumptionToken
  )

def _add_url_param(url, name, value):
  return '{}{}{}={}'.format(url, '&' if '?' in url else '?', name, value)

def _wrap_oai(content):
  return OAI_PMH_OPEN_TAG + content + OAI_PMH_CLOSE_TAG

def _wrap_with_resumption_token(content, resumption_token=None):
  return content if resumption_token is None else _format_request_token(resumption_token) + content

def _url_with_resumption_token(url, resumption_token=None):
  return _add_url_param(url, 'resumptionToken', resumption_token) if resumption_token else url

def _wrap_response(content, resumption_token=None):
  return _wrap_oai(_wrap_with_resumption_token(content, resumption_token=resumption_token))

def _set_lists_xml(content):
  return '<ListSets>{}</ListSets>'.format(content)

def _set_xml(spec, name):
  return '<set><setSpec>{}</setSpec><setName>{}</setName></set>'.format(
    spec, name
  )

def _set_list_identifiers_xml(content):
  return '<ListIdentifiers>{}</ListIdentifiers>'.format(content)

def _set_list_identifier_xml(identifier):
  return '<header><identifier>{}</identifier></header>'.format(
    identifier
  )

class TestCrossRefOaiApiIterSets(object):
  def test_should_return_empty_result_if_no_sets_present(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SETS_URL: _wrap_response(_set_lists_xml(''))
    }))
    assert list(api.iter_sets()) == []

  def test_should_return_one_result_if_one_set_is_present(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SETS_URL: _wrap_response(_set_lists_xml(
        _set_xml(SET_ID_1, NAME_1)
      ))
    }))
    assert list(api.iter_sets()) == [{
      'set_id': SET_ID_1,
      'name': NAME_1
    }]

  def test_should_return_multiple_results(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SETS_URL: _wrap_response(_set_lists_xml(''.join([
        _set_xml(SET_ID_1, NAME_1),
        _set_xml(SET_ID_2, NAME_2)
      ])))
    }))
    assert list(api.iter_sets()) == [{
      'set_id': SET_ID_1,
      'name': NAME_1
    }, {
      'set_id': SET_ID_2,
      'name': NAME_2
    }]

  def test_should_combine_two_chunks(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SETS_URL: _wrap_response(
        _set_lists_xml(''.join([
          _set_xml(SET_ID_1, NAME_1)
        ])),
        resumption_token=RESUMPTION_TOKEN_1
      ),
      _url_with_resumption_token(OAI_SETS_URL, RESUMPTION_TOKEN_1): _wrap_response(
        _set_lists_xml(''.join([
          _set_xml(SET_ID_2, NAME_2)
        ]))
      )
    }))
    assert list(api.iter_sets()) == [{
      'set_id': SET_ID_1,
      'name': NAME_1
    }, {
      'set_id': SET_ID_2,
      'name': NAME_2
    }]

class TestCrossRefOaiApiIterSetIds(object):
  def test_should_return_multiple_set_ids(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SETS_URL: _wrap_response(_set_lists_xml(''.join([
        _set_xml(SET_ID_1, NAME_1),
        _set_xml(SET_ID_2, NAME_2)
      ])))
    }))
    assert list(api.iter_set_ids()) == [
      SET_ID_1, SET_ID_2
    ]

class TestCrossRefOaiApiIterWorkIdsInSet(object):
  def test_should_return_empty_result_if_no_sets_present(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SET_IDS_URL_1: _wrap_response(_set_list_identifiers_xml(''))
    }))
    assert list(api.iter_work_ids_in_set(SET_ID_1)) == []

  def test_should_return_one_result_if_one_set_is_present(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SET_IDS_URL_1: _wrap_response(_set_list_identifiers_xml(
        _set_list_identifier_xml(WORK_ID_1)
      ))
    }))
    assert list(api.iter_work_ids_in_set(SET_ID_1)) == [
      WORK_ID_1
    ]

  def test_should_return_multiple_results(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SET_IDS_URL_1: _wrap_response(_set_list_identifiers_xml(''.join([
        _set_list_identifier_xml(WORK_ID_1),
        _set_list_identifier_xml(WORK_ID_2)
      ])))
    }))
    assert list(api.iter_work_ids_in_set(SET_ID_1)) == [
      WORK_ID_1, WORK_ID_2
    ]

  def test_should_combine_two_chunks(self):
    api = CrossRefOaiApi(get_request_handler=create_get_request_handler({
      OAI_SET_IDS_URL_1: _wrap_response(
        _set_list_identifiers_xml(''.join([
          _set_list_identifier_xml(WORK_ID_1)
        ])),
        resumption_token=RESUMPTION_TOKEN_1
      ),
      _url_with_resumption_token(OAI_SET_IDS_URL_1, RESUMPTION_TOKEN_1): _wrap_response(
        _set_list_identifiers_xml(''.join([
          _set_list_identifier_xml(WORK_ID_2)
        ]))
      )
    }))
    assert list(api.iter_work_ids_in_set(SET_ID_1)) == [
      WORK_ID_1, WORK_ID_2
    ]
