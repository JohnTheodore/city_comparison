#!/usr/bin/env python3
"""
Take Census 2017 City List, processed by data_table_census,
then the city and state to do goog geocode lookups. We get
the latitude, longitude, and reverse address. These values
are typically in the middle of the city. Then we save those
values to csv in the end. The API calls also use a cache
which is stored in the json file. You can rerun this without
hammering the API. Note, you need the GEOCODE_API_KEY
in secrets.py
"""

# pypi imports
import ssl
import sys
import certifi
import geopy

from merging_code.merge_dataframes import get_dataframe_from_merged_table_metadata
from merging_code.normalize_dataframes import add_empty_columns
from merging_code.secrets import GEOCODE_API_KEY
from merging_code.utils import get_dict_from_json_file, write_dict_to_json_file
from merging_code.utils import get_logger, write_final_dataframe, is_github_actions
from file_locations import CENSUS_FINAL_CSV_FILENAME, FBI_CRIME_COMBINED_CSV_FILENAME
from file_locations import GEOCODE_CACHED_JSON_FILENAME, GEOCODE_FINAL_CSV_FILENAME

LOGGER = get_logger('scrape_geocodes')

CSV_FILES_TO_MERGE = [{
  'csv_filename': CENSUS_FINAL_CSV_FILENAME,
  'document_label': 'census_2010',
}, {
  'csv_filename': FBI_CRIME_COMBINED_CSV_FILENAME,
  'document_label': 'fbi',
  'suffix': '_fbi_crime'
}]


def get_geopy_googlev3_locator(geocode_api_key):
  """ Get the geopy geolocator object, setup with goog auth. """
  ctx = ssl.create_default_context(cafile=certifi.where())
  geopy.geocoders.options.default_ssl_context = ctx
  # To prevent vulture complaining about unused attribute.
  assert geopy.geocoders.options.default_ssl_context == ctx
  if geocode_api_key == '' and not is_github_actions():
    sys.exit(
      'Missing geocode_api_key. Please go here -> https://console.cloud.google.com/apis/credentials. \
      Then add the API key as export GEOCODE_API_KEY="secret_code"')
  geolocator = geopy.GoogleV3(user_agent='where should I live next',
                              api_key=geocode_api_key,
                              timeout=3)
  return geolocator


def cache_geo_metadata(cached_json, location, reverse_address, search_query):
  """ Set single city geo metadata values into the main cache dict, and return the dict. """
  cached_json[search_query] = {
    'latitude': location.latitude,
    'longitude': location.longitude,
    'reverse_address': reverse_address
  }
  return cached_json


def get_reverse_address(geolocator, location):
  """ Take the lat and long, return a usps valid address string """
  reverse = geolocator.reverse('{}, {}'.format(location.latitude,
                                               location.longitude),
                               exactly_one=True)
  return reverse.address


def add_geo_metadata_to_row(row, location, reverse_address):
  """ Overwrite empty cell values in a dataframe, to add geo metadata """
  row['latitude'] = location.latitude
  row['longitude'] = location.longitude
  row['reverse_address'] = reverse_address
  return row


def add_geo_metadata_to_dataframe(dataframe):
  """ Iterate over all cities in the dataframe, then add geo metadata to all of them """
  geolocator = get_geopy_googlev3_locator(GEOCODE_API_KEY)
  cached_json = get_dict_from_json_file(GEOCODE_CACHED_JSON_FILENAME)
  add_empty_columns(dataframe, ['latitude', 'longitude', 'reverse_address'])
  api_count = 0
  # pylint: disable=W0612
  for index, row in dataframe.iterrows():
    search_query = '{}, {}, USA'.format(row['city'], row['state'])
    # make sure all 3 values are in the cache...
    if search_query in cached_json:
      # add to dataframe from cache
      city_geo_dict = cached_json[search_query]
      row[list(city_geo_dict.keys())] = list(city_geo_dict.values())
      continue
    location = geolocator.geocode(search_query)
    reverse_address = get_reverse_address(geolocator, location)
    row = add_geo_metadata_to_row(row, location, reverse_address)
    cache_geo_metadata(cached_json, location, reverse_address, search_query)
    api_count += 2  # two api hits per loop, 1 for geocode, 1 for reverse address
    if api_count % 50 == 0:
      log_msg = '### api_count: {}, cache_count: {} ###'.format(
        api_count, len(cached_json))
      LOGGER.debug(log_msg)
      write_dict_to_json_file(GEOCODE_CACHED_JSON_FILENAME, cached_json)
  write_dict_to_json_file(GEOCODE_CACHED_JSON_FILENAME, cached_json)
  return dataframe


def get_final_geocodes_dataframe():
  """ The main function which returns the final dataframe. """
  dataframe = get_dataframe_from_merged_table_metadata(LOGGER,
                                                       CSV_FILES_TO_MERGE)
  dataframe = dataframe[['city', 'state']]
  dataframe = add_geo_metadata_to_dataframe(dataframe)
  dataframe = dataframe.sort_values(by=['state', 'city'])
  return dataframe


if __name__ == '__main__':
  write_final_dataframe(LOGGER,
                        get_final_geocodes_dataframe,
                        GEOCODE_FINAL_CSV_FILENAME,
                        index=False)
