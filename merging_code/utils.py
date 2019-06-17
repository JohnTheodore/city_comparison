"""
Helper functions that are used in multiple different classes or files.
"""

import glob
import json
import os
import pandas
from merging_code.headers_cleanup import HEADERS_CHANGE
from merging_code.merge_dataframes import join_on_state_and_city


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


def drop_empty_rows_from_dataframes(dataframes, col_names):
  """ If we're missing any required information, we drop the row entirely. """
  for dataframe in dataframes:
    dataframe.dropna(axis=0, subset=col_names, inplace=True)
  return dataframes


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


def get_dataframe_from_merged_table_metadata(tables_metadata, debug=False):
  """ Take table metadata, and return a merged panda datatable. """
  combined_table = None
  for table_metadata in tables_metadata:
    if combined_table is None:
      combined_table = get_normalized_data_table(table_metadata)
      continue
    next_data_table = get_normalized_data_table(table_metadata)
    join_on_state_and_city(combined_table, next_data_table)
    print_data_table_length('combined_table', combined_table, debug=debug)
  drop_headers('final_csv', combined_table)
  rename_headers('final_csv', combined_table)

  return combined_table


def get_combined_dataframe(dataframes,
                           how='outer',
                           merge_on='city',
                           optional_merge_on='county'):
  """ Combined all the CSV files, then return the combined dataframe. """
  combined_dataframe = None
  for dataframe in dataframes:
    if combined_dataframe is None:
      combined_dataframe = dataframe
      continue
    merge_on = ['city', 'state', 'credit score']
    for optional_merge in optional_merge_on:
      if optional_merge in dataframe:
        merge_on.append(optional_merge)
    combined_dataframe = combined_dataframe.merge(dataframe,
                                                  on=merge_on,
                                                  how=how)
    print('row quantity: ', str(combined_dataframe.shape[0]))
  return combined_dataframe


def get_normalized_data_table(table_metadata, debug=False):
  """ Input a dict with csv filename, suffix if available, the document label,
  and return a data_table. """
  data_table = pandas.read_csv(table_metadata['csv_filename'],
                               header=table_metadata.get('header', 0),
                               encoding='ISO-8859-1')
  drop_headers(table_metadata['document_label'], data_table)
  rename_headers(table_metadata['document_label'], data_table)
  print_data_table_length(table_metadata['document_label'],
                          data_table,
                          debug=debug)
  # Deduplicate by ('state', 'city').
  data_table.drop_duplicates(['state', 'city'], inplace=True)
  return data_table


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


def add_empty_columns(dataframe, column_names):
  """ Add column headers with empty row values. This lets use set the values later. """
  for column_name in column_names:
    dataframe[column_name] = [''] * dataframe.shape[0]
  return dataframe


def divide_two_columns(dataframe,
                       new_col_name,
                       numerator_col,
                       divisor_col,
                       round_param=None):
  """ Divide two columns in a series. optional round_param amount. """
  divided_column = dataframe[numerator_col] / dataframe[divisor_col]
  if round_param is None:
    rounded_divided_column = divided_column.apply(lambda cell_value: round(
      cell_value, None))
  else:
    rounded_divided_column = divided_column.apply(lambda cell_value: round(
      cell_value, round_param))
  dataframe[new_col_name] = rounded_divided_column
  return dataframe


def get_all_filenames_with_extension(directory, file_ext):
  """ Get the list of all csv filenames (from the repo root). """
  all_csv_files = glob.glob('{}*{}'.format(directory, file_ext))
  return all_csv_files


def drop_headers(data_source, dataframe):
  """ Mutate the dataframe and drop the headers from HEADERS_CHANGE """
  no_headers_change = data_source not in HEADERS_CHANGE
  if no_headers_change or 'drop_columns' not in HEADERS_CHANGE[data_source]:
    return dataframe
  drop_columns = HEADERS_CHANGE[data_source]['drop_columns']
  df_columns_not_in_drop_columns = set(drop_columns) - set(dataframe.columns)
  columns_to_drop = list(set(drop_columns) - (df_columns_not_in_drop_columns))
  return dataframe.drop(labels=columns_to_drop, axis=1)


def rename_headers(data_source, dataframe):
  """ Mutate the dataframe and rename the headers from HEADERS_CHANGE """
  no_headers_change = data_source not in HEADERS_CHANGE
  if no_headers_change or 'rename_columns' not in HEADERS_CHANGE[data_source]:
    return dataframe
  return dataframe.rename(columns=HEADERS_CHANGE[data_source]['rename_columns'])


def lower_case_headers_in_dataframes(dataframe):
  """ Lower case all the strings in all the headers of the dataframes. """
  for column in dataframe.columns:
    dataframe = dataframe.rename(columns={column: column.lower()})
  return dataframe


def normalize_headers_in_dataframe(data_source, dataframe):
  """ meta function, take a single dataframe, and run the various normalizing functions on it. """
  # note, removing new lines happens _before_ renames.
  new_dataframe = remove_new_lines_from_headers_dataframes(dataframe)
  new_dataframe = lower_case_headers_in_dataframes(new_dataframe)
  new_dataframe = rename_headers(data_source, new_dataframe)
  new_dataframe = drop_headers(data_source, new_dataframe)
  new_dataframe = remove_integers_from_dataframe_by_columns(
    new_dataframe, ['city', 'state', 'county'])
  return new_dataframe


def normalize_headers_in_dataframes(data_source, dataframes):
  """ Take a list of dataframes, and run normalize_headers_in_dataframe on each dataframe. """
  new_dataframes = []
  for dataframe in dataframes:
    new_dataframes.append(normalize_headers_in_dataframe(
      data_source, dataframe))
  return new_dataframes


def remove_integers_from_dataframe_by_columns(dataframe, column_names):
  """ Remove integers from 'city' and 'state' column values.  Also make everything lowercase. """

  def remove_integers(str_val):
    """ remove integers from string, and lower. """
    if isinstance(str_val, str):
      return ''.join([i for i in str_val if not i.isdigit()]).lower()
    return str_val

  def remove_integers_from_row(row, column_name):
    """ remove integers from a row for city/state. """
    return pandas.Series([remove_integers(row[column_name])])

  for column_name in column_names:
    if column_name not in dataframe:
      continue
    dataframe[column_name] = dataframe.apply(remove_integers_from_row,
                                             column_name=column_name,
                                             axis=1)
  return dataframe


def remove_new_lines_from_headers_dataframes(dataframe):
  """ Remove all the new lines from headers in the dataframes. """
  for column in dataframe.columns:
    dataframe = dataframe.rename(columns={column: column.replace('\n', ' ')})
  return dataframe


def move_columns_to_left_of_dataframe(dataframe, new_columns_order):
  """ Helper method to take certain columns and move them to the beginning. """
  old_columns_order = list(dataframe)
  col_index = 0
  for column in new_columns_order:
    # move the column to head of list using index, pop and insert
    old_columns_order.insert(
      col_index, old_columns_order.pop(old_columns_order.index(column)))
    col_index += 1
    dataframe = dataframe.ix[:, old_columns_order]
  return dataframe


def lower_case_dataframes_columns(dataframes, columns):
  """ lowercase all the cells in the columns for all dataframes. """
  new_dataframes = []
  for dataframe in dataframes:
    new_dataframes.append(lower_case_columns(dataframe, columns))
  return new_dataframes


def lower_case_columns(dataframe, columns):
  """ lowercase all the cells in the columns for a single dataframe. """
  for column in columns:
    if column in dataframe:
      dataframe[column] = dataframe[column].str.lower()
  return dataframe
