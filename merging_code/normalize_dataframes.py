""" All code related to normalizing and cleaning up dataframes. """
import pandas
from merging_code.headers_cleanup import HEADERS_CHANGE
from merging_code.utils import remove_integers_from_string


def drop_empty_rows_from_dataframes(dataframes, col_names):
  """ If we're missing any required information, we drop the row entirely. """
  new_dataframes = []
  for dataframe in dataframes:
    new_dataframes.append(dataframe.dropna(axis=0, subset=col_names))
  return new_dataframes


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
    rounded_divided_column = divided_column.apply(
      lambda cell_value: round(cell_value, None))
  else:
    rounded_divided_column = divided_column.apply(
      lambda cell_value: round(cell_value, round_param))
  dataframe[new_col_name] = rounded_divided_column
  return dataframe


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

  def remove_integers_from_row(row, column_name):
    """ remove integers from a row for city/state. """
    return pandas.Series([remove_integers_from_string(row[column_name])])

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
