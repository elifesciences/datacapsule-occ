from __future__ import absolute_import

import logging
import json

from citerank.crossref.crossref_rest_api import (
  WORK_URL_PREFIX,
  CrossRefRestApi,
  Work
)

from citerank.crossref.test_utils import (
  create_get_request_handler
)

WORK_ID_1 = 'work:1'
WORK_ID_2 = 'work:2'

TITLE_1 = 'Title 1'
TITLE_2 = 'Title 2'

DOI_1 = 'doi/1'

def get_logger():
  return logging.getLogger(__name__)

def setup_module():
  logging.basicConfig(level='DEBUG')

class TestCrossRefRestApiGetRawWork(object):
  def test_should_return_provided_work(self):
    json_content = {
      'dummy': 'json'
    }
    api = CrossRefRestApi(get_request_handler=create_get_request_handler({
      WORK_URL_PREFIX + WORK_ID_1: json.dumps(json_content)
    }))
    assert api.get_raw_work(WORK_ID_1) == json_content

class TestWorkGetTitle(object):
  def test_should_return_none_title_if_not_provided(self):
    json_content = {
    }
    work = Work(json_content)
    assert work.get_title() is None

  def test_should_return_single_title(self):
    json_content = {
      'title': TITLE_1
    }
    work = Work(json_content)
    assert work.get_title() == TITLE_1

  def test_should_return_single_title_from_array(self):
    json_content = {
      'title': [
        TITLE_1
      ]
    }
    work = Work(json_content)
    assert work.get_title() == TITLE_1

  def test_should_join_multiple_titles_from_array(self):
    json_content = {
      'title': [
        TITLE_1,
        TITLE_2
      ]
    }
    work = Work(json_content)
    assert work.get_title() == ' '.join([TITLE_1, TITLE_2])

class TestWorkGetReferenceDois(object):
  def test_should_return_empty_list_if_not_provided(self):
    json_content = {
    }
    work = Work(json_content)
    assert work.get_reference_dois() == []

  def test_should_return_empty_list_if_doi_is_not_provided(self):
    json_content = {
      'reference': [{
      }]
    }
    work = Work(json_content)
    assert work.get_reference_dois() == []

  def test_should_return_doi_if_provided(self):
    json_content = {
      'reference': [{
        'DOI': DOI_1
      }]
    }
    work = Work(json_content)
    assert work.get_reference_dois() == [DOI_1]
