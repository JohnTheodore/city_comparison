""" Helper functions that are used in multiple different classes or files. """

import glob
import json
import logging
import os
import time
import pandas

import coloredlogs


def get_logger(logger_name):
  """ Get the python logger, add colors, and name it. """
  env_level_name = os.environ.get('LOG_LEVEL', 'INFO')
  log_level = logging.getLevelName(env_level_name)
  log_format = '%(asctime)-15s %(name)-19s %(levelname)-9s %(message)s'
  logger = logging.getLogger(logger_name)
  logger.setLevel(log_level)
  field_styles = dict(asctime=dict(color='green'),
                      hostname=dict(color='magenta'),
                      levelname=dict(color='yellow'),
                      programname=dict(color='cyan'),
                      name=dict(color='white'))
  coloredlogs.install(fmt=log_format,
                      level=log_level,
                      logger=logger,
                      field_styles=field_styles)
  return logger


def stop_watch_function(logger, function):
  """ Run a function, log how long it took, then return it's value. """
  logger.info('Starting {}'.format(function.__name__))
  start_time = time.time()
  result = function()
  elapsed_time = round(time.time() - start_time)
  log_msg = 'Finished running: {} in {} seconds'.format(function.__name__,
                                                        elapsed_time)
  logger.info(log_msg)
  return result


def write_final_dataframe(logger, function, file_name, index=True):
  """ A function to get the final dataframe, log the results, and write the file. """
  final_dataframe = stop_watch_function(logger, function)
  final_dataframe.to_csv(file_name, index=index)
  logger.info('Wrote file: {}'.format(file_name))


def get_dict_from_json_file(filename):
  """ Read json file, loads it, and return the dict """
  if os.path.isfile(filename):
    with open(filename) as file_handler:
      text = file_handler.read()
      return json.loads(text)
  return {}


def write_dict_to_json_file(filename, cached_json):
  """ Take a python dict, write it to a file as json.dumps. """
  with open(filename, 'w') as file_handler:
    file_handler.write(json.dumps(cached_json))


def get_dataframe_from_spreadsheet(logger,
                                   file_path,
                                   sheet_type='csv',
                                   **kwargs):
  """ Get the list of all csv filenames (from the repo root). """
  log_msg = 'Reading spreadsheet file: {}, sheet_type: {}, **kwargs: {}'.format(
    file_path, sheet_type, kwargs)
  logger.debug(log_msg)
  if sheet_type == 'xls':
    return pandas.read_excel(file_path, **kwargs)
  if sheet_type == 'csv':
    return pandas.read_csv(file_path, **kwargs)
  return None


def remove_substring_from_end_of_string(input_string, substring_list):
  """ func('foo bar baz', [' baz', ' bar']) outputs 'foo'. """
  new_string = input_string
  for substring in substring_list:
    if new_string.endswith(substring):
      new_string = new_string[:(-1 * (len(substring)))]
  return new_string


def get_all_filenames_with_extension(directory, file_ext):
  """ Get the list of all csv filenames (from the repo root). """
  all_csv_files = glob.glob('{}*{}'.format(directory, file_ext))
  return all_csv_files


def remove_integers_from_string(str_val):
  """ Remove integers from string, and lower. """
  if isinstance(str_val, str):
    return ''.join([i for i in str_val if not i.isdigit()]).lower()
  return str_val
