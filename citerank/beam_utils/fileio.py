import logging
import os

from apache_beam.io.filebasedsink import FileBasedSinkWriter
from apache_beam.pvalue import EmptySideInput
from apache_beam.io.textio import WriteToText

def get_logger():
  return logging.getLogger(__name__)

# The WorkaroundWriteToText may be needed in combination with FnApiRunner
# (`init_result` would then be EmptySideInput instead of the temp path)
class WorkaroundWriteToText(WriteToText, object):
  def __init__(self, *args, **kwargs):
    append_uid = kwargs.pop('append_uid', True)
    super(WorkaroundWriteToText, self).__init__(*args, **kwargs)


    def open_writer(sink, init_result, uid):
      get_logger().warn('append_uid: %s', append_uid)
      file_path_prefix = sink.file_path_prefix.get()
      file_name_suffix = sink.file_name_suffix.get()
      if not append_uid:
        file_name = file_path_prefix + file_name_suffix
      elif isinstance(init_result, EmptySideInput):
        file_name = file_path_prefix + '.' + uid + file_name_suffix
      else:
        suffix = (
          '.' + os.path.basename(file_path_prefix) + file_name_suffix
        )
        get_logger().debug('init_result: %s', init_result)
        get_logger().debug('uid: %s', uid)
        file_name = os.path.join(init_result, uid) + suffix
      get_logger().info('file_name: %s', file_name)
      return FileBasedSinkWriter(sink, file_name)

    self._sink.open_writer = lambda init_result, uid: open_writer(self._sink, init_result, uid)
