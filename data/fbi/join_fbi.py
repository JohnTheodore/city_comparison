""" Join Census and FBI data into one combined pandas DataFrame. """

import pandas
from headers_cleanup import drop_headers, rename_headers
from data_sources import FBI_CRIME_2014_XLS_FILENAME, FBI_CRIME_2015_XLS_FILENAME
from data_sources import FBI_CRIME_2016_XLS_FILENAME, FBI_CRIME_2017_XLS_FILENAME
from data_sources import FBI_CRIME_COMBINED_CSV_FILENAME
from data_table import DataTable


class Fbi(DataTable):
  """ Table of FBI data. """

  @staticmethod
  def read(file_path):
    data = pandas.read_excel(file_path, header=3)

    # Remove empty columns.
    # data.drop(data.columns[[13, 14, 15, 16, 17, 18]], axis=1, inplace=True)

    # Replace the '\n' in header names and make lower_case:
    # "Murder and\nnonnegligent\nmanslaughter" =>
    # "murder and nonnegligent manslaughter"
    def normalize_header(header):
      return header.lower().replace('\n', ' ')

    data = data.rename(columns=normalize_header)

    # Remove integers from 'city' and 'state' column values.  Also make
    # everything lowercase.
    def remove_integers(str_val):
      if isinstance(str_val, str):
        return ''.join([i for i in str_val if not i.isdigit()]).lower()
      return str_val

    def remove_integers_from_row(row):
      return pandas.Series(
        [remove_integers(row['city']),
         remove_integers(row['state'])])

    data[['city', 'state']] = data.apply(remove_integers_from_row, axis=1)

    # Propagate 'state' column.
    state = None
    for i, row in data.iterrows():
      if pandas.notnull(row['state']):
        state = row['state']
      data.at[i, 'state'] = state

    # Normalize all numeric columns by population.
    numeric_columns = data.select_dtypes(
      include=['float64', 'int64']).columns.to_list()
    # Don't normalize 'population' column.
    numeric_columns = [col for col in numeric_columns if col != 'population']

    def normalize_by_pop100k(row):
      # Divide by 100k.
      population = row['population']
      # For every numeric column (excluding 'city' and 'state', which are string
      # columns), normalize to crime per 100k population.
      new_columns = {}
      for column in numeric_columns:
        assert isinstance(row[column], int) or isinstance(row[column], float)
        if population > 0:
          new_columns[column] = row[column] / population * 1e5
        else:
          new_columns[column] = None

      return pandas.Series(new_columns)

    data[numeric_columns] = data.apply(normalize_by_pop100k, axis=1)

    return data

  @staticmethod
  def get_exact_matching_key():
    # By returning `None` as key, we use `index` as key.
    # return None
    return ['state', 'city']

  @staticmethod
  def get_state_key():
    return 'state'

  @staticmethod
  def get_city_key():
    return 'city'

  @staticmethod
  def get_population_key():
    return None


def debug_print_dataframe(data, num_rows=2, debug=False):
  """ If debug enabled, print a few rows from pandas DataFrame. """
  if debug:
    with pandas.option_context('display.max_rows', None, 'display.max_columns',
                               None):
      print(data[:num_rows])


def get_normalized_data_table(table_metadata, debug=False):
  """ Input a dict with csv filename, suffix if available, the document label,
  and return a data_table. """
  suffix = table_metadata.get('suffix', '')
  data_table = table_metadata['table_class'](
    file_path=table_metadata['csv_filename'], suffix=suffix)
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


def get_dataframe_from_merged_csv_files(tables_metadata, debug=False):
  """ Join Census data with FBI data and write out CSV. """
  combined_table = None
  for table_metadata in tables_metadata:
    if combined_table is None:
      combined_table = get_normalized_data_table(table_metadata)
      continue
    next_data_table = get_normalized_data_table(table_metadata)
    how = table_metadata.get('how', 'inner')
    combined_table = combined_table.join(next_data_table, how=how)
    print_data_table_length('combined_table', combined_table.data, debug=debug)
  drop_headers('final_csv', combined_table.data)
  rename_headers('final_csv', combined_table.data)
  return combined_table.data


if __name__ == '__main__':

  FBI_FILES_TO_MERGE = [{
    'csv_filename': FBI_CRIME_2014_XLS_FILENAME,
    'document_label': 'fbi_2014',
    'table_class': Fbi,
    'suffix': ' fbi_2014',
    'how': 'outer',
    'year': 2014,
  }, {
    'csv_filename': FBI_CRIME_2015_XLS_FILENAME,
    'document_label': 'fbi_2015',
    'table_class': Fbi,
    'suffix': ' fbi_2015',
    'how': 'outer',
    'year': 2015,
  }, {
    'csv_filename': FBI_CRIME_2016_XLS_FILENAME,
    'document_label': 'fbi_2016',
    'table_class': Fbi,
    'suffix': ' fbi_2016',
    'how': 'outer',
    'year': 2016,
  }, {
    'csv_filename': FBI_CRIME_2017_XLS_FILENAME,
    'document_label': 'fbi_2017',
    'table_class': Fbi,
    'suffix': ' fbi_2017',
    'how': 'outer',
    'year': 2017,
  }]

  FBI_DATAFRAMES = {
    table_metadata['year']: get_normalized_data_table(table_metadata).data
    for table_metadata in FBI_FILES_TO_MERGE
  }

  # Add year column for each FBI dataframe.  Note that we're using a
  # "multi-index" of (state, city, year).
  for year, df in FBI_DATAFRAMES.items():
    df['year'] = year
    df.set_index(['state', 'city', 'year'], inplace=True)

  # Concatenate the data from years 2014-2017.
  COMBINED = pandas.concat(FBI_DATAFRAMES.values(), sort=True)
  # Take average states over year.  Now index is (state, city).
  COMBINED_MEAN = COMBINED.mean(level=[0, 1])
  COMBINED_MEAN.to_csv(FBI_CRIME_COMBINED_CSV_FILENAME, index=True)
