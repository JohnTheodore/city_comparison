#!/usr/bin/env python3
"""
Get the walk/bike/transit scores for every city from the census, store the raw values
in the cache, and write the csv. We use the csv from geocode in order to have the
right lat/long/address
"""
import time
import requests
from merging_code.normalize_dataframes import add_empty_columns
from merging_code.utils import get_dict_from_json_file, write_dict_to_json_file, get_dataframe_from_spreadsheet
from merging_code.secrets import WALKSCORE_API_KEY
from file_locations import WALKSCORE_FINAL_CSV_FILENAME, WALKSCORE_CACHED_JSON_FILENAME, GEOCODE_FINAL_CSV_FILENAME


def get_mobility_scores_dict(walkscore_json, state_city_name):
  """ Take raw walkscore json response and return a simple dictionary
  of the 3 scores for each city. When missing we use ''. """
  summary_scores_dict = {'walkscore': '', 'bikescore': '', 'transitscore': ''}
  if walkscore_json['status'] == 1:
    summary_scores_dict['walkscore'] = walkscore_json['walkscore']
  else:
    print(state_city_name, ': missing walkscore.')
  for score in ['transit', 'bike']:
    if score in walkscore_json:
      summary_scores_dict['{}score'.format(
        score)] = walkscore_json[score]['score']
  return summary_scores_dict


def get_mobility_scores_from_api(city_row):
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


def add_mobility_scores_to_row(row, walkscore_json, city_state_string):
  """ Input a dataframe row without walkscores. Output a dataframe row with walkscores. """
  summary_scores_dict = get_mobility_scores_dict(walkscore_json,
                                                 city_state_string)
  row[list(summary_scores_dict.keys())] = list(summary_scores_dict.values())
  return row


def extract_useful_keys_from_dict(walkscore_dict):
  """ Extract only the useful mobility scores from the walkscore.com json. We don't want the cache to be too big. """
  mobility_scores = [
    'walkscore', 'bike', 'transit', 'status', 'snapped_lat', 'snapped_lon',
    'description', 'updated'
  ]
  result = {}
  for key, value in walkscore_dict.items():
    if key in mobility_scores:
      result[key] = value
  return result


def add_mobility_scores_to_dataframe(dataframe):
  """ Parse every city row from the geocode csv, add the walkscores cells to each. """
  cached_dict = get_dict_from_json_file(WALKSCORE_CACHED_JSON_FILENAME)
  add_empty_columns(dataframe, ['walkscore', 'bikescore', 'transitscore'])
  api_count = 0
  for index, row in dataframe.iterrows():
    city_state_string = '{}, {}'.format(row['city'], row['state'])
    # make sure all 3 values are in the cache...
    if city_state_string in cached_dict:
      # add to dataframe from cache
      walkscore_dict = cached_dict[city_state_string]
      cached_dict[city_state_string] = extract_useful_keys_from_dict(
        walkscore_dict)
      dataframe.loc[index] = add_mobility_scores_to_row(row, walkscore_dict,
                                                        city_state_string)
      continue
    walkscore_dict = get_mobility_scores_from_api(row)
    walkscore_dict = extract_useful_keys_from_dict(walkscore_dict)
    cached_dict[city_state_string] = walkscore_dict
    dataframe.loc[index] = add_mobility_scores_to_row(row, walkscore_dict,
                                                      city_state_string)
    api_count += 2  # two api hits per loop, 1 for geocode, 1 for reverse address
    if api_count % 10 == 0:
      print('cached_dict count: ', str(len(cached_dict.keys())))
      write_dict_to_json_file(WALKSCORE_CACHED_JSON_FILENAME, cached_dict)
  write_dict_to_json_file(WALKSCORE_CACHED_JSON_FILENAME, cached_dict)


def get_final_dataframe():
  """ The main function which returns the final dataframe. """
  dataframe = get_dataframe_from_spreadsheet(GEOCODE_FINAL_CSV_FILENAME)
  add_mobility_scores_to_dataframe(dataframe)
  return dataframe


if __name__ == '__main__':
  print('Starting write_walkscores_csv.py with api key: ', WALKSCORE_API_KEY)
  DATAFRAME = get_final_dataframe()
  DATAFRAME = DATAFRAME.sort_values(by=['state', 'city'])
  print('Writing row quantity: ', DATAFRAME['city'].count())
  DATAFRAME.to_csv(WALKSCORE_FINAL_CSV_FILENAME, index=False)
  print('Finished writing: ', WALKSCORE_FINAL_CSV_FILENAME)
