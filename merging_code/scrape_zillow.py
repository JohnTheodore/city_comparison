#!/usr/bin/env python3
"""
Get the housing data for each row in GEOCODE_FINAL_CSV_FILENAME.
"""
import time
import pandas
import quandl
from file_locations import GEOCODE_FINAL_CSV_FILENAME, ZILLOW_CACHED_JSON_FILENAME
from file_locations import CITY_CODES_CSV_FILENAME, ZILLOW_FINAL_CSV_FILENAME
from merging_code.merge_dataframes import join_on_state_and_city
from merging_code.normalize_dataframes import add_empty_columns, normalize_headers_in_dataframe
from merging_code.utils import get_dict_from_json_file, write_dict_to_json_file, get_dataframe_from_spreadsheet
from merging_code.utils import get_logger, write_final_dataframe
from merging_code.secrets import QUANDL_API_KEY

LOGGER = get_logger('scrape_zillow')

US_STATES_DICT = {
  'al': 'alabama',
  'ak': 'alaska',
  'az': 'arizona',
  'ar': 'arkansas',
  'ca': 'california',
  'co': 'colorado',
  'ct': 'connecticut',
  'de': 'delaware',
  'fl': 'florida',
  'ga': 'georgia',
  'hi': 'hawaii',
  'id': 'idaho',
  'il': 'illinois',
  'in': 'indiana',
  'ia': 'iowa',
  'ks': 'kansas',
  'ky': 'kentucky',
  'la': 'louisiana',
  'me': 'maine',
  'md': 'maryland',
  'ma': 'massachusetts',
  'mi': 'michigan',
  'mn': 'minnesota',
  'ms': 'mississippi',
  'mo': 'missouri',
  'mt': 'montana',
  'ne': 'nebraska',
  'nv': 'nevada',
  'nh': 'new hampshire',
  'nj': 'new jersey',
  'nm': 'new mexico',
  'ny': 'new york',
  'nc': 'north carolina',
  'nd': 'north dakota',
  'oh': 'ohio',
  'ok': 'oklahoma',
  'or': 'oregon',
  'pa': 'pennsylvania',
  'ri': 'rhode island',
  'sc': 'south carolina',
  'sd': 'south dakota',
  'tn': 'tennessee',
  'tx': 'texas',
  'ut': 'utah',
  'vt': 'vermont',
  'va': 'virginia',
  'wa': 'washington',
  'wv': 'west virginia',
  'wi': 'wisconsin',
  'wy': 'wyoming',
  'dc': 'dc'
}

ZILLOW_PRICE_CODES = {
  'ZRIFAH': 'Zillow Rental Index Per Square Foot - All Homes'
}

ZILLOW_CACHE = get_dict_from_json_file(ZILLOW_CACHED_JSON_FILENAME)


def quandl_get_dataframe(quandl_get_value, api_count):
  """ Wrapper for quandl.get, to avoid the exception raising. and use caching. """
  # get the value from the cache
  if quandl_get_value in ZILLOW_CACHE:
    quandl_value = ZILLOW_CACHE[quandl_get_value]
    if quandl_value is not None:
      quandl_value = pandas.DataFrame(quandl_value)
  # query quandl
  else:
    api_count += 1
    try:
      quandl_value = quandl.get(quandl_get_value,
                                collapse='annual',
                                order='desc',
                                api_key=QUANDL_API_KEY)
      quandl_value = quandl_value.reset_index()
      quandl_value['Date'] = quandl_value['Date'].dt.strftime('%Y-%m-%d')
    except quandl.errors.quandl_error.NotFoundError:
      quandl_value = None
  if quandl_value is None:
    # write the value to the cache
    ZILLOW_CACHE[quandl_get_value] = None
    return (None, api_count)
  zillow_dataframe = quandl_value.sort_values(by=['Date'], ascending=False)
  ZILLOW_CACHE[quandl_get_value] = zillow_dataframe.to_dict()
  return (zillow_dataframe, api_count)


def add_zillow_price_code_to_row(row, city_code, zillow_price_code, api_count):
  """ Take a row, add the zillow price code, return the row. """
  quandl_get_value = 'ZILLOW/C{}_{}'.format(city_code, zillow_price_code)
  quandl_value, api_count = quandl_get_dataframe(quandl_get_value, api_count)
  if quandl_value is None:
    return (row, api_count, False)
  value = quandl_value['Value'].iloc[0]
  row[zillow_price_code] = value
  return (row, api_count, True)


def add_zillow_price_codes_to_row(row, api_count):
  """ Query quandl for all the housing data we want for each city/state. return pandas dataframe. """
  city_codes = row.get('city_code').split('|')
  working_city_code = False
  for city_code in city_codes:
    for zillow_price_code, description in ZILLOW_PRICE_CODES.items():
      log_msg = '{}, {} {{yesno}} {} found for city code: {}'.format(
        row.get('city'), row.get('state'), description, city_code)
      row, api_count, working_city_code = add_zillow_price_code_to_row(
        row, city_code, zillow_price_code, api_count)
      if working_city_code:
        LOGGER.debug(log_msg.format(yesno=''))
      else:
        LOGGER.debug(log_msg.format(yesno='NOT'))
    # if we got one working city_code for a city, we skip the rest.
    if working_city_code:
      return (row, api_count)
  return (row, api_count)


def add_zillow_price_codes_to_dataframe(dataframe):
  """ Parse every city row from the geocode csv, add the walkscores cells to each. """
  api_count = 0
  for index, row in dataframe.iterrows():
    new_row, api_count = add_zillow_price_codes_to_row(row, api_count)
    dataframe.loc[index] = new_row
    api_pause = 20
    if api_count > 0 and api_count % api_pause == 0:
      log_msg = '### api_count: {}, cache_count: {} ###'.format(
        api_count, len(ZILLOW_CACHE))
      LOGGER.debug(log_msg)
      write_dict_to_json_file(ZILLOW_CACHED_JSON_FILENAME, ZILLOW_CACHE)
      # the quandl api limit is 2,000 calls per 10 minutes
      # That's ~3.3 calls per second. We'll sleep so we don't get throttled.
      time.sleep(api_pause / 3.3)
  return dataframe


def normalize_city_codes_dataframe(dataframe):
  """ Add city, state and county columns, fill in the values. Lowercase everything.
  When we find duplicate cities with multiple rows, we merge them, and join the
  city code with a |. """

  def parse_city_county_state(area):
    """ Return a pandas series with the city, county and state, this is used by apply.
    eg, area could be:  'Amesbury, Essex, MA' """
    area = area.strip().lower()
    area_list = area.split(', ')
    assert len(area_list) < 4
    city = area_list[0]
    county = ''
    # if there are more than 2 values, the 2nd value is the county.
    if len(area_list) > 2:
      county = area_list[1]
    state = US_STATES_DICT[area_list[-1]]
    return pandas.Series([city, county, state])

  new_cols = ['city', 'county', 'state']
  dataframe[new_cols] = dataframe['AREA'].apply(parse_city_county_state)
  # unfortunately, there are multiple codes for the same city
  # and some of them don't work.
  dataframe['CODE'] = dataframe['CODE'].apply(str)
  dataframe['CODE'] = dataframe['CODE'].str.strip()
  dataframe = dataframe.groupby(['city', 'state'])['CODE'].apply('|'.join)
  dataframe = dataframe.reset_index()
  dataframe = normalize_headers_in_dataframe('zillow_city_codes', dataframe)
  return dataframe


def get_final_zillow_dataframe():
  """ The main function which returns the final dataframe. """
  city_codes_dataframe = get_dataframe_from_spreadsheet(LOGGER,
                                                        CITY_CODES_CSV_FILENAME,
                                                        delimiter='|')
  city_codes_dataframe = normalize_city_codes_dataframe(city_codes_dataframe)
  geocodes_dataframe = get_dataframe_from_spreadsheet(
    LOGGER, GEOCODE_FINAL_CSV_FILENAME)
  combined_dataframe = join_on_state_and_city(LOGGER, geocodes_dataframe,
                                              city_codes_dataframe)
  combined_dataframe = add_empty_columns(combined_dataframe,
                                         ZILLOW_PRICE_CODES.keys())
  combined_dataframe = combined_dataframe.reset_index()
  final_dataframe = add_zillow_price_codes_to_dataframe(combined_dataframe)
  final_dataframe = final_dataframe.drop(['index'], axis=1)
  final_dataframe = final_dataframe.sort_values(by=['state', 'city'])
  return final_dataframe


if __name__ == '__main__':
  write_final_dataframe(LOGGER,
                        get_final_zillow_dataframe,
                        ZILLOW_FINAL_CSV_FILENAME,
                        index=False)
