""" Join Census and FBI data into one combined pandas DataFrame. """

import pandas
from headers_cleanup import drop_headers, rename_headers
from data_sources import FBI_CRIME_2014_CSV_FILENAME, FBI_CRIME_2015_CSV_FILENAME
from data_sources import FBI_CRIME_2016_CSV_FILENAME, FBI_CRIME_2017_CSV_FILENAME
from data_sources import FBI_CRIME_COMBINED_CSV_FILENAME
from data_table_fbi import Fbi as fbi_data_table


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
    'csv_filename': FBI_CRIME_2014_CSV_FILENAME,
    'document_label': 'fbi_2014',
    'table_class': fbi_data_table,
    'suffix': ' fbi_2014',
    'how': 'outer',
    'year': 2014,
  }, {
    'csv_filename': FBI_CRIME_2015_CSV_FILENAME,
    'document_label': 'fbi_2015',
    'table_class': fbi_data_table,
    'suffix': ' fbi_2015',
    'how': 'outer',
    'year': 2015,
  }, {
    'csv_filename': FBI_CRIME_2016_CSV_FILENAME,
    'document_label': 'fbi_2016',
    'table_class': fbi_data_table,
    'suffix': ' fbi_2016',
    'how': 'outer',
    'year': 2016,
  }, {
    'csv_filename': FBI_CRIME_2017_CSV_FILENAME,
    'document_label': 'fbi_2017',
    'table_class': fbi_data_table,
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
  COMBINED = pandas.concat(FBI_DATAFRAMES.values()).sort_index()
  # Take average states over year.  Now index is (state, city).
  COMBINED_MEAN = COMBINED.mean(level=[0, 1])
  COMBINED_MEAN.to_csv(FBI_CRIME_COMBINED_CSV_FILENAME, index=True)
