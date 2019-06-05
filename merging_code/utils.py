"""
Helper functions that are used in multiple different classes or files.
"""

import glob
import json
import os
import pandas
from merging_code.data_table import DataTable
from merging_code.headers_cleanup import drop_headers, rename_headers


def read_json_file(filename):
  """ Read json file, loads it, and return the dict """
  if os.path.isfile(filename):
    with open(filename) as file_handler:
      text = file_handler.read()
      return json.loads(text)
  return {}


def write_json_file(filename, cached_json):
  """ Take a python dict, write it to a file as json.dumps. """
  with open(filename, 'w') as file_handler:
    file_handler.write(json.dumps(cached_json))


def get_dataframe_from_spreadsheet(file_path,
                                   header=0,
                                   encoding='ISO-8859-1',
                                   sheet_type='csv'):
  """ Get the list of all csv filenames (from the repo root). """
  if sheet_type == 'xls':
    return pandas.read_excel(file_path, encoding=encoding, header=header)
  if sheet_type == 'csv':
    return pandas.read_csv(file_path, encoding=encoding, header=header)
  return None


def remove_substring_from_end_of_string(input_string, substring_list):
  """ func('foo bar baz', [' baz', ' bar']) outputs 'foo'. """
  new_string = input_string
  for substring in substring_list:
    if new_string.endswith(substring):
      # import ipdb
      # ipdb.set_trace()
      new_string = new_string[:(-1 * (len(substring)))]
  return new_string


def get_dataframe_from_merged_table_metadata(tables_metadata, debug=False):
  """ Take table metadata, and return a merged panda datatable. """
  combined_table = None
  for table_metadata in tables_metadata:
    if combined_table is None:
      combined_table = get_normalized_data_table(table_metadata)
      continue
    next_data_table = get_normalized_data_table(table_metadata)
    combined_table = combined_table.join(next_data_table)
    print_data_table_length('combined_table', combined_table.data, debug=debug)
  drop_headers('final_csv', combined_table.data)
  rename_headers('final_csv', combined_table.data)
  return combined_table.data


def get_normalized_data_table(table_metadata, debug=False):
  """ Input a dict with csv filename, suffix if available, the document label,
  and return a data_table. """
  suffix = table_metadata.get('suffix', '')
  data_table = ParseCsvData(file_path=table_metadata['csv_filename'],
                            suffix=suffix,
                            header=table_metadata.get('header', 0))
  drop_headers(table_metadata['document_label'], data_table.data)
  rename_headers(table_metadata['document_label'], data_table.data)
  print_data_table_length(table_metadata['document_label'],
                          data_table.data,
                          debug=debug)
  return data_table


def print_data_table_length(document_label, data_frame, debug=False):
  """ A helper print function for seeing the table row length. """
  print('{}\n'.format(document_label), len(data_frame))
  debug_print_dataframe(data_frame, debug=debug)


def debug_print_dataframe(data, num_rows=2, debug=False):
  """ If debug enabled, print a few rows from pandas DataFrame. """
  if debug:
    with pandas.option_context('display.max_rows', None, 'display.max_columns',
                               None):
      print(data[:num_rows])


def add_empty_columns(dataframe, column_names):
  """ Add column headers with empty row values. This lets use set the values later. """
  for column_name in column_names:
    dataframe[column_name] = [''] * dataframe.shape[0]


def get_all_filenames_with_extension(directory, file_ext):
  """ Get the list of all csv filenames (from the repo root). """
  all_csv_files = glob.glob('{}*{}'.format(directory, file_ext))
  return all_csv_files


class ParseCsvData(DataTable):
  """ Table of csv data. """

  @staticmethod
  def read(file_path):
    data = get_dataframe_from_spreadsheet(file_path)
    return data

  @staticmethod
  def get_exact_matching_key():
    # By returning `None` as key, we use `index` as key.
    # return None
    return 'index'

  @staticmethod
  def get_state_key():
    return 'state'

  @staticmethod
  def get_city_key():
    return 'city'

  @staticmethod
  def get_population_key():
    return None
