""" Join Census and FBI data into one combined pandas DataFrame. """

from file_locations import CENSUS_FINAL_CSV_FILENAME, FBI_CRIME_COMBINED_CSV_FILENAME
from file_locations import EXPERIAN_FINAL_CSV_FILENAME
from file_locations import WALKSCORE_FINAL_CSV_FILENAME, MASTER_CSV_FILENAME
from merging_code.headers_cleanup import drop_headers, rename_headers
from merging_code.utils import get_normalized_data_table, print_data_table_length


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
    'csv_filename': CENSUS_FINAL_CSV_FILENAME,
    'document_label': 'census_2010',
  }, {
    'csv_filename': WALKSCORE_FINAL_CSV_FILENAME,
    'document_label': 'walkscore',
    'suffix': '_walkscore'
  }, {
    'csv_filename': FBI_CRIME_COMBINED_CSV_FILENAME,
    'document_label': 'fbi_2017',
    'suffix': '_fbi_crime'
  }, {
    'csv_filename': EXPERIAN_FINAL_CSV_FILENAME,
    'document_label': 'experian_2017',
    'suffix': 'experian_2017'
  }]
  # Set debug to True to print out 2 rows out of each dataframe.
  COMBINED_DATAFRAME = get_dataframe_from_merged_csv_files(CSV_FILES_TO_MERGE,
                                                           debug=False)
  # Write the combined dataframe table to the final csv file.
  COMBINED_DATAFRAME.to_csv(MASTER_CSV_FILENAME, index_label='delme')
  print('Wrote file: ', MASTER_CSV_FILENAME)
