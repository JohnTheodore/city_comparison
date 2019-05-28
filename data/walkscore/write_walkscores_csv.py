#!/usr/bin/env python3
"""
Get the walk/bike/transit scores for every city from the census, store the raw values
in the cache, and write the csv. We use the csv from geocode in order to have the
right lat/long/address
"""
import time
from secrets import WALKSCORE_API_KEY
import pandas
import requests
from utils import read_json_file, write_json_file
from data_sources import WALKSCORE_FINAL_CSV_FILENAME, WALKSCORE_CACHED_JSON_FILENAME


def get_geocode_dataframe():
  """ Get the geocode dataframe from the ./data/geocode final csv file. """
  geo_table = pandas.read_csv('data/geocode/cities_geo_lat_long_address.csv')
  return geo_table


def add_empty_columns(dataframe, column_names):
  """ Add empty columns so we can set the values of individual cells later. """
  for column_name in column_names:
    dataframe[column_name] = [''] * dataframe['city'].count()


def get_summary_scores_dict(walkscore_json, state_city_name):
  """ Take raw walkscore json response and return a simple dictionary
  of the 3 scores for each city. When missing we use ''. """
  summary_scores_dict = {'walkscore': '', 'bikescore': '', 'transitscore': ''}
  if walkscore_json['status'] == 1:
    summary_scores_dict['walkscore'] = walkscore_json['walkscore']
    if 'transit' in walkscore_json:
      summary_scores_dict['transitscore'] = walkscore_json['transit']['score']
    if 'bike' in walkscore_json:
      summary_scores_dict['bikescore'] = walkscore_json['bike']['score']
  else:
    print(state_city_name, ': missing walkscore.')
  return summary_scores_dict


def get_walkscores(city_row):
  """ Get the walkscore json result from the walkscore api. """
  # We hardcode a sleep time of 3 between every request, this works.
  time.sleep(3)
  address = city_row['reverse_address']
  lat = city_row['latitude']
  long = city_row['longitude']
  scheme = 'https://'
  fqdn = 'api.walkscore.com'
  url = '{}{}/score?format=json&address={}&lat={}&lon={}&transit=1&bike=1&wsapikey={}'.format(
    scheme, fqdn, address, lat, long, WALKSCORE_API_KEY)
  result = requests.get(url)
  result.raise_for_status()
  return result.json()


def get_dataframe_row_with_walkscores(row, walkscore_json):
  """ Input a dataframe row without walkscores. Output a dataframe row with walkscores. """
  summary_scores_dict = get_summary_scores_dict(walkscore_json,
                                                get_state_city_name(row))
  row[list(summary_scores_dict.keys())] = list(summary_scores_dict.values())
  return row


def get_state_city_name(row):
  """ A helper to get the california_sunnyvale string from a dataframe row. """
  return '{}, {}'.format(row['city'], row['state'])


def add_walkscore_to_cities(dataframe):
  """ Parse every city row from the geocode csv, add the walkscores cells to each. """
  cached_json = read_json_file(WALKSCORE_CACHED_JSON_FILENAME)
  add_empty_columns(dataframe, ['walkscore', 'bikescore', 'transitscore'])
  api_count = 0
  for index, row in dataframe.iterrows():
    state_city_name = get_state_city_name(row)
    # make sure all 3 values are in the cache...
    if state_city_name in cached_json:
      # add to dataframe from cache
      if 'help_link' in cached_json[state_city_name]:
        cached_json[state_city_name].pop('help_link')
      walkscore_json = cached_json[state_city_name]
      dataframe.loc[index] = get_dataframe_row_with_walkscores(
        row, walkscore_json)
      continue
    walkscore_json = get_walkscores(row)
    cached_json[state_city_name] = walkscore_json
    dataframe.loc[index] = get_dataframe_row_with_walkscores(
      row, walkscore_json)
    api_count += 2  # two api hits per loop, 1 for geocode, 1 for reverse address
    if api_count % 10 == 0:
      print('cached_json count: ', str(len(cached_json.keys())))
      write_json_file(WALKSCORE_CACHED_JSON_FILENAME, cached_json)
  write_json_file(WALKSCORE_CACHED_JSON_FILENAME, cached_json)


DATAFRAME = get_geocode_dataframe()
add_walkscore_to_cities(DATAFRAME)
DATAFRAME.to_csv(WALKSCORE_FINAL_CSV_FILENAME, index=False)
