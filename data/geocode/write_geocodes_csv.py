#!/usr/bin/env python3

import json
import os
import ssl
from data_table_census import Census as census_data_table
from secrets import goog_geocoding_api_key
import certifi
import geopy

GEO_CACHED_JSON_FILENAME = './data/geocode/geo_data.json'


def get_census_cities_and_states():
  census_population_2017_table = census_data_table(
    file_path='data/census/PEP_2017_PEPANNRSIP.US12A_with_ann.csv')
  dataframe = census_population_2017_table.data
  dataframe = dataframe[['city', 'state']]
  return dataframe


def get_goog_geolocator(goog_geocoding_api_key):
  ctx = ssl.create_default_context(cafile=certifi.where())
  geopy.geocoders.options.default_ssl_context = ctx
  geolocator = geopy.GoogleV3(user_agent='where should I live next',
                              api_key=goog_geocoding_api_key,
                              timeout=3)
  return geolocator


def add_empty_columns(dataframe, column_names):
  for column_name in column_names:
    dataframe[column_name] = [''] * dataframe['city'].count()


def append_to_geo_cache(cached_json, location, reverse_address, search_query):
  cached_json[search_query] = {
    'latitude': location.latitude,
    'longitude': location.longitude,
    'reverse_address': reverse_address
  }
  return cached_json


def get_reverse_address(geolocator, location):
  reverse = geolocator.reverse('{}, {}'.format(location.latitude,
                                               location.longitude),
                               exactly_one=True)
  return reverse.address


def append_location_metadata_to_row(row, location, reverse_address):
  row['latitude'] = location.latitude
  row['longitude'] = location.longitude
  row['reverse_address'] = reverse_address


def add_lat_lon_to_cities(dataframe):
  geolocator = get_goog_geolocator(goog_geocoding_api_key)
  cached_json = get_cached_json(GEO_CACHED_JSON_FILENAME)
  add_empty_columns(dataframe, ['latitude', 'longitude', 'reverse_address'])
  api_count = 0
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
    append_location_metadata_to_row(row, location, reverse_address)
    append_to_geo_cache(cached_json, location, reverse_address, search_query)
    api_count += 2  # two api hits per loop, 1 for geocode, 1 for reverse address
    if api_count % 50 == 0:
      print('API count: ', str(api_count))
      write_cached_json(GEO_CACHED_JSON_FILENAME, cached_json)
  write_cached_json(GEO_CACHED_JSON_FILENAME, cached_json)


def get_cached_json(filename):
  if os.path.isfile(filename):
    with open(GEO_CACHED_JSON_FILENAME) as file_handler:
      text = file_handler.read()
      return json.loads(text)
  return {}


def write_cached_json(filename, cached_json):
  with open(GEO_CACHED_JSON_FILENAME, 'w') as file_handler:
    file_handler.write(json.dumps(cached_json))


dataframe = get_census_cities_and_states()
add_lat_lon_to_cities(dataframe)
dataframe.to_csv('data/geocode/cities_geo_lat_long_address.csv', index=False)
