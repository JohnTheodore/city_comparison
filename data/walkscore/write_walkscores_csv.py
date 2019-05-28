#!/usr/bin/env python3

import json
import os
import time
from secrets import walkscore_api_key
import pandas
import requests
CACHED_JSON_FILENAME = './data/walkscore/walkscore_data.json'


def get_geo_dataframe():
  geo_table = pandas.read_csv('data/geocode/cities_geo_lat_long_address.csv')
  return geo_table


def add_empty_columns(dataframe, column_names):
  for column_name in column_names:
    dataframe[column_name] = [''] * dataframe['city'].count()


def get_summary_scores_dict(walkscore_json, state_city_name):
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
  time.sleep(3)
  address = city_row['reverse_address']
  lat = city_row['latitude']
  long = city_row['longitude']
  url = 'http://api.walkscore.com/score?format=json&address={}&lat={}&lon={}&transit=1&bike=1&wsapikey={}'.format(
    address, lat, long, walkscore_api_key)
  result = requests.get(url)
  result.raise_for_status()
  return result.json()


def get_summary_scores_to_row(row, walkscore_json):
  summary_scores_dict = get_summary_scores_dict(walkscore_json,
                                                get_state_city_name(row))
  row[list(summary_scores_dict.keys())] = list(summary_scores_dict.values())
  return row


def get_state_city_name(row):
  return '{}, {}'.format(row['city'], row['state'])


def add_walkscore_to_cities(dataframe):
  cached_json = get_cached_json(CACHED_JSON_FILENAME)
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
      dataframe.loc[index] = get_summary_scores_to_row(row, walkscore_json)
      continue
    walkscore_json = get_walkscores(row)
    cached_json[state_city_name] = walkscore_json
    dataframe.loc[index] = get_summary_scores_to_row(row, walkscore_json)
    api_count += 2  # two api hits per loop, 1 for geocode, 1 for reverse address
    if api_count % 10 == 0:
      print('cached_json count: ', str(len(cached_json.keys())))
      write_cached_json(CACHED_JSON_FILENAME, cached_json)
  write_cached_json(CACHED_JSON_FILENAME, cached_json)


def get_cached_json(filename):
  if os.path.isfile(filename):
    with open(CACHED_JSON_FILENAME) as file_handler:
      text = file_handler.read()
      return json.loads(text)
  return {}


def write_cached_json(filename, cached_json):
  with open(CACHED_JSON_FILENAME, 'w') as file_handler:
    file_handler.write(json.dumps(cached_json))


dataframe = get_geo_dataframe()
add_walkscore_to_cities(dataframe)
dataframe.to_csv('data/walkscore/cities_walkscores.csv', index=False)
