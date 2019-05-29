""" Join Census and FBI data into one combined pandas DataFrame. """

import pandas
from headers_cleanup import drop_headers, rename_headers
from data_sources import CENSUS_POPULATION_2017_CSV_FILENAME, CENSUS_AREA_2010_CSV_FILENAME
from data_sources import FBI_CRIME_2017_CSV_FILENAME, EXPERIAN_FINAL_CSV_FILENAME
from data_sources import WALKSCORE_FINAL_CSV_FILENAME, MASTER_CSV_FILENAME
from data_table_census import Census as census_data_table
from data_table_fbi import Fbi as fbi_data_table
from data_table_experian import Experian as experian_data_table
from data_table_walkscore import Walkscore as walkscore_data_table


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
    combined_table = combined_table.join(next_data_table)
    print_data_table_length('combined_table', combined_table.data, debug=debug)
  drop_headers('final_csv', combined_table.data)
  rename_headers('final_csv', combined_table.data)
  return combined_table.data


if __name__ == '__main__':
  CSV_FILES_TO_MERGE = [{
    'csv_filename': CENSUS_POPULATION_2017_CSV_FILENAME,
    'document_label': 'census_2017',
    'table_class': census_data_table
  }, {
    'csv_filename': CENSUS_AREA_2010_CSV_FILENAME,
    'document_label': 'census_2010',
    'table_class': census_data_table
  }, {
    'csv_filename': WALKSCORE_FINAL_CSV_FILENAME,
    'document_label': 'walkscore',
    'table_class': walkscore_data_table,
    'suffix': '_walkscore'
  }, {
    'csv_filename': FBI_CRIME_2017_CSV_FILENAME,
    'document_label': 'fbi_2017',
    'table_class': fbi_data_table,
    'suffix': '_fbi_crime'
  }, {
    'csv_filename': EXPERIAN_FINAL_CSV_FILENAME,
    'document_label': 'experian_2017',
    'table_class': experian_data_table,
    'suffix': 'experian_2017'
  }]
  # Set debug to True to print out 2 rows out of each dataframe.
COMBINED_DATAFRAME = get_dataframe_from_merged_csv_files(CSV_FILES_TO_MERGE,
                                                         debug=False)
# Write the combined dataframe table to the final csv file.
COMBINED_DATAFRAME.to_csv(MASTER_CSV_FILENAME, index_label='delme')
print('Wrote file: ', MASTER_CSV_FILENAME)
