""" Join Census and FBI data into one combined pandas DataFrame. """

from file_locations import CDC_FINAL_CSV_FILENAME
from file_locations import CENSUS_FINAL_CSV_FILENAME
from file_locations import ELECTIONS_FINAL_CSV_FILENAME
from file_locations import FBI_CRIME_COMBINED_CSV_FILENAME
from file_locations import MASTER_CSV_FILENAME
from file_locations import WALKSCORE_FINAL_CSV_FILENAME
from file_locations import ZILLOW_FINAL_CSV_FILENAME
from merging_code.headers_cleanup import HEADERS_CHANGE
from merging_code.merge_dataframes import get_dataframe_from_merged_table_metadata
from merging_code.merge_dataframes import JoinColumn
from merging_code.normalize_dataframes import move_columns_to_left_of_dataframe, divide_two_columns, drop_headers
from merging_code.utils import get_logger, write_final_dataframe

LOGGER = get_logger('get_city_comparison')

CSV_FILES_TO_MERGE = [{
  'csv_filename': CENSUS_FINAL_CSV_FILENAME,
  'document_label': 'census_2010',
  'join_column': JoinColumn.STATE_CITY
}, {
  'csv_filename': WALKSCORE_FINAL_CSV_FILENAME,
  'document_label': 'walkscore',
  'suffix': '_walkscore',
  'join_column': JoinColumn.STATE_CITY
}, {
  'csv_filename': FBI_CRIME_COMBINED_CSV_FILENAME,
  'document_label': 'fbi',
  'suffix': '_fbi_crime',
  'join_column': JoinColumn.STATE_CITY
}, {
  'csv_filename': ZILLOW_FINAL_CSV_FILENAME,
  'document_label': 'zillow_final',
  'suffix': 'zillow',
  'join_column': JoinColumn.STATE_CITY
}, {
  'csv_filename': CDC_FINAL_CSV_FILENAME,
  'document_label': 'cdc',
  'join_column': JoinColumn.COUNTY_FIPS
}, {
  'csv_filename': ELECTIONS_FINAL_CSV_FILENAME,
  'document_label': 'elections_2020',
  'join_column': JoinColumn.COUNTY_FIPS
}]


def get_final_city_comparison_dataframe():
  """ The main function which returns the final dataframe. """
  # `get_dataframe_from_merged_table_metadata` joins on `state` and `city`.
  dataframe = get_dataframe_from_merged_table_metadata(LOGGER,
                                                       CSV_FILES_TO_MERGE)
  land_area_key = HEADERS_CHANGE['census_2010']['rename_columns'][
    'area in square miles - land area']
  dataframe = divide_two_columns(dataframe, 'population density', 'population',
                                 land_area_key)
  dataframe = move_columns_to_left_of_dataframe(
    dataframe, ['city', 'state', 'population density'])
  dataframe = drop_headers('final_csv', dataframe)
  dataframe = dataframe.sort_values(by=['state', 'city'])

  # The CDC (Center for Disease Control) and NVSS (National Vital Statistics
  # System) index much of their data by county (especially county FIPS), so we
  # have to perform a "left merge" mapping "state, city" to county.

  return dataframe


if __name__ == '__main__':
  # Write the combined dataframe table to the final csv file.
  write_final_dataframe(LOGGER,
                        get_final_city_comparison_dataframe,
                        MASTER_CSV_FILENAME,
                        index=False)
