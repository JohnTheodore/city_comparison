""" Join Census and FBI data into one combined pandas DataFrame. """

import pandas
from file_locations import FBI_CRIME_2014_XLS_FILENAME, FBI_CRIME_2015_XLS_FILENAME
from file_locations import FBI_CRIME_2016_XLS_FILENAME, FBI_CRIME_2017_XLS_FILENAME
from file_locations import FBI_CRIME_COMBINED_CSV_FILENAME
from merging_code.headers_cleanup import drop_headers, rename_headers

FBI_FILES_TO_MERGE = [{
  'xls_filename': FBI_CRIME_2014_XLS_FILENAME,
  'document_label': 'fbi_2014',
  'suffix': ' fbi_2014',
  'how': 'outer',
  'year': 2014,
}, {
  'xls_filename': FBI_CRIME_2015_XLS_FILENAME,
  'document_label': 'fbi_2015',
  'suffix': ' fbi_2015',
  'how': 'outer',
  'year': 2015,
}, {
  'xls_filename': FBI_CRIME_2016_XLS_FILENAME,
  'document_label': 'fbi_2016',
  'suffix': ' fbi_2016',
  'how': 'outer',
  'year': 2016,
}, {
  'xls_filename': FBI_CRIME_2017_XLS_FILENAME,
  'document_label': 'fbi_2017',
  'suffix': ' fbi_2017',
  'how': 'outer',
  'year': 2017,
}]


def get_dataframe_from_fbi_excel_file(table_metadata):
  """ Get the dataframe from an excel filename. """
  dataframe = pandas.read_excel(table_metadata['xls_filename'], header=3)

  # Remove empty columns.
  # dataframe.drop(dataframe.columns[[13, 14, 15, 16, 17, 18]], axis=1, inplace=True)

  # Replace the '\n' in header names and make lower_case:
  # "Murder and\nnonnegligent\nmanslaughter" =>
  # "murder and nonnegligent manslaughter"
  def normalize_header(header):
    return header.lower().replace('\n', ' ')

  dataframe = dataframe.rename(columns=normalize_header)

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

  dataframe[['city', 'state']] = dataframe.apply(remove_integers_from_row,
                                                 axis=1)

  drop_headers(table_metadata['document_label'], dataframe)
  rename_headers(table_metadata['document_label'], dataframe)
  # remove the crap / notes at the bottom of the dataframe.
  dataframe = dataframe[dataframe.population.notnull()]
  # Propagate 'state' column.
  state = None
  for i, row in dataframe.iterrows():
    if pandas.notnull(row['state']):
      state = row['state']
    dataframe.at[i, 'state'] = state

  # Normalize all numeric columns by population.
  numeric_columns = dataframe.select_dtypes(
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
      assert isinstance(row[column], (int, float))
      if population > 0:
        new_columns[column] = row[column] / population * 1e5
      else:
        new_columns[column] = None

    return pandas.Series(new_columns)

  dataframe[numeric_columns] = dataframe.apply(normalize_by_pop100k, axis=1)

  return dataframe


def get_final_dataframe():
  """ The main function which returns the final dataframe with all merged/meaned fbi xls files. """
  fbi_dataframes = {
    table_metadata['year']: get_dataframe_from_fbi_excel_file(table_metadata)
    for table_metadata in FBI_FILES_TO_MERGE
  }
  # Add year column for each FBI dataframe.  Note that we're using a
  # "multi-index" of (state, city, year).

  for year, dataframe in fbi_dataframes.items():
    dataframe['year'] = year
    dataframe.set_index(['state', 'city', 'year'], inplace=True)

  combined = pandas.concat(fbi_dataframes.values(), sort=True)
  # Take average states over year.  Now index is (state, city).
  combined_mean = combined.mean(level=[0, 1])
  return combined_mean


if __name__ == '__main__':
  get_final_dataframe().to_csv(FBI_CRIME_COMBINED_CSV_FILENAME, index=True)
