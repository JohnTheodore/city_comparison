""" Join Census and FBI data into one combined pandas DataFrame. """

import datetime
import math
import pandas
from file_locations import FBI_CRIME_2014_XLS_FILENAME, FBI_CRIME_2015_XLS_FILENAME
from file_locations import FBI_CRIME_2016_XLS_FILENAME, FBI_CRIME_2017_XLS_FILENAME
from file_locations import FBI_CRIME_COMBINED_CSV_FILENAME
from merging_code.utils import get_dataframe_from_spreadsheet
from merging_code.normalize_dataframes import drop_empty_rows_from_dataframes, lower_case_columns
from merging_code.normalize_dataframes import normalize_headers_in_dataframe


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


def normalize_dataframe_by_pop100k(dataframe):
  """ Normalize a dataframe of FBI crime data by 100k for the year population. """
  # Normalize all numeric columns by population.
  numeric_columns = dataframe.select_dtypes(
    include=['float64', 'int64']).columns.to_list()
  # Don't normalize 'population' or 'year' column.
  numeric_columns = [
    col for col in numeric_columns if col not in ('population', 'year')
  ]

  dataframe[numeric_columns] = dataframe.apply(normalize_row_by_pop100k,
                                               numeric_columns=numeric_columns,
                                               axis=1)
  return dataframe


def add_state_to_missing_state_col_cells(dataframe):
  """ FBI xls files are messed up, this adds the state where missing on the state column. """
  # Propagate 'state' column.
  state = None
  for index, row in dataframe.iterrows():
    if pandas.notnull(row['state']):
      state = row['state'].lower()
      dataframe.at[index, 'state'] = state
    dataframe.at[index, 'state'] = state

  return dataframe


def get_earliest_and_latest_fbi_dataframe(dataframe):
  """Take the data from the latest year it was defined."""
  dataframe = dataframe.reset_index()
  dataframe = dataframe.set_index(['state', 'city'])

  # Transform the integer 'year' column to type `datetime`.  'datetime' column
  # will allow us to use `pandas.DataFrame.first` and `pandas.DataFrame.last`.
  def convert_row_year_str_to_datetime_object(row):
    return datetime.datetime(int(row['year']), 12, 31)

  dataframe['datetime'] = dataframe.apply(
    convert_row_year_str_to_datetime_object, axis=1)
  groupby = dataframe.groupby(['state', 'city'])
  first = groupby.first()
  last = groupby.last()
  # Drop the 'datetime' column to clean up.
  first = first.drop(['datetime'], axis=1)
  last = last.drop(['datetime'], axis=1)
  return (first, last)


def get_annualized_percent_change_per_year(diff, years):
  """Calculate annualized percent change."""
  return math.exp(math.log(diff) / years) - 1


def get_series_with_annual_percent_change_for_columns(row, numeric_columns):
  """ Show the annual change for a list of columns in a row. """
  years = row['year']
  new_columns = {}
  for column in numeric_columns:
    diff = row[column]
    if years > 0 and diff != 0:
      new_columns[column] = get_annualized_percent_change_per_year(diff, years)
    else:
      new_columns[column] = 0
  return pandas.Series(new_columns)


def add_annual_percent_change_for_numeric_fields(dataframe):
  """ Calculate annual percent change for numeric fields in Dataframe. """
  numeric_columns = dataframe.select_dtypes(
    include=['float64', 'int64']).columns.to_list()
  # Don't calculate annual percent change on 'year' column.
  numeric_columns = [
    col for col in numeric_columns if not col.startswith('year')
  ]
  dataframe[numeric_columns] = dataframe.apply(
    get_series_with_annual_percent_change_for_columns,
    numeric_columns=numeric_columns,
    axis=1)
  return dataframe


def get_dataframe_with_annual_pop_percent_chg(first, last):
  """ Compute annualized percent change in population.

  Based on the first and last years we have data.

  Args:
    first: `pandas.DataFrame` indexed by ('state', 'city'), with population data
      from earliest year we have data for.
    last: `pandas.DataFrame` indexed by ('state', 'city'), with population data
      from latest year we have data for.

  Returns:
    `pandas.DataFrame` with column 'population' representing the annual percent
    change in population.  Indexed by ('state', 'city').
  """
  diff_year = last['year'].to_frame().subtract(first['year'].to_frame())
  # Add epsilon to values in order to avoid dividing by zero.
  epsilon = 0.01
  last_population = last['population'].to_frame()
  first_population = first['population'].to_frame()
  population_ratio = (last_population + epsilon).div(first_population + epsilon)
  population_ratio = population_ratio.merge(diff_year,
                                            how='inner',
                                            left_index=True,
                                            right_index=True,
                                            suffixes=('_ratio', '_diff'))
  population_percent_change = add_annual_percent_change_for_numeric_fields(
    population_ratio)
  population_percent_change = population_percent_change.drop(['year'], axis=1)
  return population_percent_change


def get_concatenated_fbi_dataframe_from_xls_files(fbi_table_metadata):
  """ Take the FBI Table 8 xls files, normalize them, then return a list of them all. """
  normalized_fbi_dataframes = []
  for table_metadata in fbi_table_metadata:
    document_label = table_metadata['document_label']
    dataframe = get_dataframe_from_spreadsheet(table_metadata['xls_filename'],
                                               header=3,
                                               sheet_type='xls')
    dataframe = normalize_headers_in_dataframe(document_label, dataframe)
    dataframe = add_state_to_missing_state_col_cells(dataframe)
    dataframe = normalize_dataframe_by_pop100k(dataframe)
    dataframe = lower_case_columns(dataframe, ['city', 'state'])
    dataframe['year'] = table_metadata['year']
    dataframe = dataframe.set_index(['state', 'city', 'year'])
    normalized_fbi_dataframes.append(dataframe)
  # There are some rows in the FBI xls spreadsheet that are notes, and not
  # actually data from cities.  Drop these rows by only keeping rows that have
  # population field defined.
  normalized_fbi_dataframes = drop_empty_rows_from_dataframes(
    normalized_fbi_dataframes, ['population'])
  return pandas.concat(normalized_fbi_dataframes, sort=True)


def average_and_round_combined_fbi_dataframe(concatenated_fbi_dataframe):
  """ Average and round all the the crime numbers for the concatenated fbi dataframe. """
  combined_mean = concatenated_fbi_dataframe.mean(level=[0, 1])
  combined_mean_rounded = combined_mean.round(1)
  return combined_mean_rounded


def get_final_dataframe():
  """ The main function which returns the final dataframe with all merged/meaned fbi xls files. """
  fbi_table_metadata = get_fbi_table_metadata()
  combined_fbi_dataframe = get_concatenated_fbi_dataframe_from_xls_files(
    fbi_table_metadata)
  combined_mean_rounded = average_and_round_combined_fbi_dataframe(
    combined_fbi_dataframe)
  # Compute annualized percent change in population, based on the
  # first and last years we have data.
  first, last = get_earliest_and_latest_fbi_dataframe(combined_fbi_dataframe)
  population_percent_change = get_dataframe_with_annual_pop_percent_chg(
    first, last)

  # Add 'population_percent_change' column to `combined_mean_rounded`.
  # Adds '_mean' suffix to the columns in `combined_mean_rounded`.
  result = population_percent_change.merge(combined_mean_rounded,
                                           how='inner',
                                           left_index=True,
                                           right_index=True,
                                           suffixes=('_percent_change',
                                                     '_mean'))
  # Add 'population' column to `result`.  'population' column contains
  # population data from the latest year we have data for.
  result = result.merge(last['population'].to_frame(),
                        how='inner',
                        left_index=True,
                        right_index=True,
                        suffixes=('_mean', ''))

  # Drop the mean population, it won't be used.
  result = result.drop(['population_mean'], axis=1)
  result = result.sort_values(by=['city', 'state'])
  return result


if __name__ == '__main__':
  get_final_dataframe().to_csv(FBI_CRIME_COMBINED_CSV_FILENAME, index=True)
