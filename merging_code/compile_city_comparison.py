""" Join Census and FBI data into one combined pandas DataFrame. """

from file_locations import CENSUS_FINAL_CSV_FILENAME, FBI_CRIME_COMBINED_CSV_FILENAME
from file_locations import EXPERIAN_FINAL_CSV_FILENAME, ZILLOW_FINAL_CSV_FILENAME
from file_locations import WALKSCORE_FINAL_CSV_FILENAME, MASTER_CSV_FILENAME
from merging_code.headers_cleanup import HEADERS_CHANGE
from merging_code.merge_dataframes import get_dataframe_from_merged_table_metadata
from merging_code.normalize_dataframes import move_columns_to_left_of_dataframe, divide_two_columns, drop_headers

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
}, {
  'csv_filename': ZILLOW_FINAL_CSV_FILENAME,
  'document_label': 'zillow_final',
  'suffix': 'zillow'
}]


def get_final_dataframe():
  """ The main function which returns the final dataframe. """
  dataframe = get_dataframe_from_merged_table_metadata(CSV_FILES_TO_MERGE,
                                                       debug=False)
  land_area_key = HEADERS_CHANGE['census_2010']['rename_columns'][
    'area in square miles - land area']
  dataframe = divide_two_columns(dataframe, 'population density', 'population',
                                 land_area_key)
  dataframe = move_columns_to_left_of_dataframe(
    dataframe, ['city', 'state', 'population density'])
  dataframe = drop_headers('final_csv', dataframe)
  dataframe = dataframe.sort_values(by=['state', 'city'])
  return dataframe


if __name__ == '__main__':
  # Write the combined dataframe table to the final csv file.
  get_final_dataframe().to_csv(MASTER_CSV_FILENAME, index=False)
  print('Wrote file: ', MASTER_CSV_FILENAME)
