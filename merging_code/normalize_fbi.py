""" Join Census and FBI data into one combined pandas DataFrame. """

import pandas
from file_locations import FBI_CRIME_2014_XLS_FILENAME, FBI_CRIME_2015_XLS_FILENAME
from file_locations import FBI_CRIME_2016_XLS_FILENAME, FBI_CRIME_2017_XLS_FILENAME
from file_locations import FBI_CRIME_COMBINED_CSV_FILENAME
from merging_code.utils import get_dataframe_from_spreadsheet, normalize_headers_in_dataframe
from merging_code.utils import drop_empty_rows_from_dataframes, lower_case_columns


def get_fbi_table_metadata():
  """ This table_metadata is used to iterate over all 4 fbi xls crime files. """
  return [{
    'xls_filename': FBI_CRIME_2014_XLS_FILENAME,
    'document_label': 'fbi_2014',
    'year': 2014,
  }, {
    'xls_filename': FBI_CRIME_2015_XLS_FILENAME,
    'document_label': 'fbi_2015',
    'year': 2015,
  }, {
    'xls_filename': FBI_CRIME_2016_XLS_FILENAME,
    'document_label': 'fbi_2016',
    'year': 2016,
  }, {
    'xls_filename': FBI_CRIME_2017_XLS_FILENAME,
    'document_label': 'fbi_2017',
    'year': 2017,
  }]


def remove_integers_from_cells_by_column(dataframe, column_names):
  """ Remove integers from 'city' and 'state' column values.  Also make everything lowercase. """

  def remove_integers(str_val):
    """ remove integers from string, and lower. """
    if isinstance(str_val, str):
      return ''.join([i for i in str_val if not i.isdigit()]).lower()
    return str_val

  def remove_integers_from_row(row):
    """ remove integers from a row for city/state. """
    return pandas.Series(
      [remove_integers(row['city']),
       remove_integers(row['state'])])

  for column_name in column_names:
    dataframe[column_name] = dataframe.apply(remove_integers_from_row,
                                             args=column_name,
                                             axis=1)
  return dataframe


def normalize_row_by_pop100k(row, numeric_columns):
  """ Normalize a row of FBI crime data by 100k for the year population. """
  # Divide by 100k.
  population = row['population']
  # For every numeric column (excluding 'city' and 'state', which are string
  # columns), normalize to crime per 100k population.
  new_columns = {}
  for column in numeric_columns:
    assert isinstance(row[column], (int, float))
    if population > 0:
      new_columns[column] = row[column] / population * 1e5
    else:
      new_columns[column] = None
  return pandas.Series(new_columns)


def normalize_dataframe_by_100k(dataframe):
  """ Normalize a dataframe of FBI crime data by 100k for the year population. """
  # Normalize all numeric columns by population.
  numeric_columns = dataframe.select_dtypes(
    include=['float64', 'int64']).columns.to_list()
  # Don't normalize 'population' column.
  numeric_columns = [col for col in numeric_columns if col != 'population']

  dataframe[numeric_columns] = dataframe.apply(normalize_row_by_pop100k,
                                               numeric_columns=numeric_columns,
                                               axis=1)
  return dataframe


def propagate_state_column_to_dataframe(dataframe):
  """ FBI xls files are messed up, this adds the state where missing on the first column. """
  # Propagate 'state' column.
  state = None
  for index, row in dataframe.iterrows():
    if pandas.notnull(row['state']):
      state = row['state'].lower()
      dataframe.at[index, 'state'] = state
    dataframe.at[index, 'state'] = state

  return dataframe


def get_final_dataframe():
  """ The main function which returns the final dataframe with all merged/meaned fbi xls files. """
  fbi_table_metadata = get_fbi_table_metadata()
  normalized_fbi_dataframes = []
  for table_metadata in fbi_table_metadata:
    document_label = table_metadata['document_label']
    dataframe = get_dataframe_from_spreadsheet(table_metadata['xls_filename'],
                                               header=3,
                                               sheet_type='xls')
    dataframe = normalize_headers_in_dataframe(document_label, dataframe)
    dataframe = propagate_state_column_to_dataframe(dataframe)
    dataframe['year'] = table_metadata['year']
    dataframe = normalize_dataframe_by_100k(dataframe)
    dataframe = lower_case_columns(dataframe, ['city', 'state'])
    dataframe.set_index(['state', 'city', 'year'], inplace=True)
    normalized_fbi_dataframes.append(dataframe)

  normalized_fbi_dataframes = drop_empty_rows_from_dataframes(
    normalized_fbi_dataframes, ['population'])

  combined = pandas.concat(normalized_fbi_dataframes, sort=True)
  # Take average states over year.  Now index is (state, city).
  combined_mean = combined.mean(level=[0, 1])
  combined_mean_rounded = combined_mean.round(1)
  return combined_mean_rounded


if __name__ == '__main__':
  get_final_dataframe().to_csv(FBI_CRIME_COMBINED_CSV_FILENAME, index=True)
