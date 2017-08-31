import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from citerank.beam_utils.fileio import (
  WorkaroundWriteToText
)

TEST_OUTPUT_FILE = '.temp/test.csv'


def get_logger():
  return logging.getLogger(__name__)

def create_fn_api_runner():
  from apache_beam.runners.portability.fn_api_runner import FnApiRunner
  return FnApiRunner()

def setup_module():
  logging.basicConfig(level='DEBUG')

class TestFnApiRunnerWriteToText(object):
  def test_write(self):
    pipeline_options = PipelineOptions([])

    runner = create_fn_api_runner()

    with beam.Pipeline(runner, options=pipeline_options) as p:
      output = p | beam.Create(['DUMMY'])
      output |= "Write Output" >> WorkaroundWriteToText(
        TEST_OUTPUT_FILE,
        append_uid=False
      )
