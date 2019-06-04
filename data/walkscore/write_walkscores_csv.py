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
  # I have no idea what is wrong with walkscore, but https doesn't work. only http.
  scheme = 'http://'
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


def remove_attributes_from_dict(walkscore_dict):
  """ Make the dict smaller so we don't store too much data. """
  attributes = [
    'more_info_icon', 'logo_url', 'more_info_link', 'ws_link', 'help_link'
  ]
  for attribute in attributes:
    if attribute in walkscore_dict:
      walkscore_dict.pop(attribute)
  return walkscore_dict


def add_walkscore_to_cities(dataframe):
  """ Parse every city row from the geocode csv, add the walkscores cells to each. """
  cached_dict = read_json_file(WALKSCORE_CACHED_JSON_FILENAME)
  add_empty_columns(dataframe, ['walkscore', 'bikescore', 'transitscore'])
  api_count = 0
  for index, row in dataframe.iterrows():
    state_city_name = get_state_city_name(row)
    # make sure all 3 values are in the cache...
    if state_city_name in cached_dict:
      # add to dataframe from cache
      walkscore_dict = cached_dict[state_city_name]
      cached_dict[state_city_name] = remove_attributes_from_dict(walkscore_dict)
      dataframe.loc[index] = get_dataframe_row_with_walkscores(
        row, walkscore_dict)
      continue
    walkscore_dict = get_walkscores(row)
    remove_attributes_from_dict(walkscore_dict)
    cached_dict[state_city_name] = walkscore_dict
    dataframe.loc[index] = get_dataframe_row_with_walkscores(
      row, walkscore_dict)
    api_count += 2  # two api hits per loop, 1 for geocode, 1 for reverse address
    if api_count % 10 == 0:
      print('cached_dict count: ', str(len(cached_dict.keys())))
      write_json_file(WALKSCORE_CACHED_JSON_FILENAME, cached_dict)
  write_json_file(WALKSCORE_CACHED_JSON_FILENAME, cached_dict)


if __name__ == '__main__':
  print('Starting write_walkscores_csv.py with api key: ', WALKSCORE_API_KEY)
  DATAFRAME = get_geocode_dataframe()
  add_walkscore_to_cities(DATAFRAME)
  DATAFRAME.to_csv(WALKSCORE_FINAL_CSV_FILENAME, index=False)
  print('Finished write_walkscores_csv.py')
