"""
Helper functions that are used in multiple different classes or files.
These should be non-pandas related functions only.
"""

import glob
import json
import os
import pandas


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


def get_dataframe_from_spreadsheet(file_path, sheet_type='csv', **kwargs):
  """ Get the list of all csv filenames (from the repo root). """
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


# remove everything below this line


def print_data_table_length(document_label, dataframe, debug=False):
  """ A helper print function for seeing the table row length. """
  print('{}\n'.format(document_label), len(dataframe))
  debug_print_dataframe(dataframe, debug=debug)


def debug_print_dataframe(data, num_rows=2, debug=False):
  """ If debug enabled, print a few rows from pandas DataFrame. """
  if debug:
    with pandas.option_context('display.max_rows', None, 'display.max_columns',
                               None):
      print(data[:num_rows])
