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
# standard imports
import ssl

from secrets import GEOCODE_API_KEY

# pypi imports
import certifi
import geopy

from data_table_census import Census as census_data_table
from utils import read_json_file, write_json_file
from data_sources import GEOCODE_CACHED_JSON_FILENAME, CENSUS_POPULATION_2017_CSV_FILENAME, GEOCODE_FINAL_CSV_FILENAME


def get_census_cities_and_states_dataframe():
  """ Load the pandas dataframe from data_table_census. (2017) """
  census_population_2017_table = census_data_table(
    file_path=CENSUS_POPULATION_2017_CSV_FILENAME)
  dataframe = census_population_2017_table.data
  dataframe = dataframe[['city', 'state']]
  return dataframe


def get_goog_geolocator(geocode_api_key):
  """ Get the geopy geolocator object, setup with goog auth. """
  ctx = ssl.create_default_context(cafile=certifi.where())
  geopy.geocoders.options.default_ssl_context = ctx
  geolocator = geopy.GoogleV3(user_agent='where should I live next',
                              api_key=geocode_api_key,
                              timeout=3)
  return geolocator


def add_empty_columns(dataframe, column_names):
  """ Instantiate column headers with empty row values. This lets use set the values later. """
  for column_name in column_names:
    dataframe[column_name] = [''] * dataframe['city'].count()


def set_geo_metadata_to_dict(cached_json, location, reverse_address,
                             search_query):
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


def set_geo_metadata_to_dataframe_row(row, location, reverse_address):
  """ Overwrite empty cell values in a dataframe, to add geo metadata """
  row['latitude'] = location.latitude
  row['longitude'] = location.longitude
  row['reverse_address'] = reverse_address


def set_geo_metadata_to_dataframe(dataframe):
  """ Iterate over all cities in the dataframe, then add geo metadata to all of them """
  geolocator = get_goog_geolocator(GEOCODE_API_KEY)
  cached_json = read_json_file(GEOCODE_CACHED_JSON_FILENAME)
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
    set_geo_metadata_to_dataframe_row(row, location, reverse_address)
    set_geo_metadata_to_dict(cached_json, location, reverse_address,
                             search_query)
    api_count += 2  # two api hits per loop, 1 for geocode, 1 for reverse address
    if api_count % 50 == 0:
      print('API count: ', str(api_count))
      write_json_file(GEOCODE_CACHED_JSON_FILENAME, cached_json)
  write_json_file(GEOCODE_CACHED_JSON_FILENAME, cached_json)


DATAFRAME = get_census_cities_and_states_dataframe()
set_geo_metadata_to_dataframe(DATAFRAME)
DATAFRAME.to_csv(GEOCODE_FINAL_CSV_FILENAME, index=False)
