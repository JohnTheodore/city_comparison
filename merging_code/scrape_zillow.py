#!/usr/bin/env python3
"""
Get the housing data for each row in GEOCODE_FINAL_CSV_FILENAME.
"""
import time
import pandas
from termcolor import cprint
import quandl
from file_locations import GEOCODE_FINAL_CSV_FILENAME
from file_locations import CITY_CODES_CSV_FILENAME, ZILLOW_FINAL_CSV_FILENAME
from merging_code.merge_dataframes import join_on_state_and_city
from merging_code.utils import get_dataframe_from_spreadsheet, add_empty_columns, normalize_headers_in_dataframe
from merging_code.secrets import QUANDL_API_KEY

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


def add_zillow_price_code_to_row(row, city_code, zillow_price_code):
  """ Take a row, add the zillow price code, return the row. """
  quandl_get_value = 'ZILLOW/C{}_{}'.format(city_code, zillow_price_code)
  zillow_dataframe = quandl.get(quandl_get_value,
                                collapse='annual',
                                order='desc',
                                api_key=QUANDL_API_KEY)
  value = zillow_dataframe['Value'].iloc[0]
  row[zillow_price_code] = value
  return row


def add_housing_data_to_row(row):
  """ Query quandl for all the housing data we want for each city/state. return pandas dataframe. """
  city_codes = row.get('city_code').split('|')
  working_city_code = False
  city_codes_len = len(city_codes)
  city_codes_not_working_counter = 0
  for city_code in city_codes:
    for zillow_price_code, description in ZILLOW_PRICE_CODES.items():
      log_msg = '{}, {} {{yesno}} {} found for city code: {}'.format(
        row.get('city'), row.get('state'), description, city_code)
      try:
        row = add_zillow_price_code_to_row(row, city_code, zillow_price_code)
        cprint(log_msg.format(yesno=''), 'green')
        working_city_code = True
      # There are many duplicate city codes for each city, and a lot of the city codes
      # do not work.
      except quandl.errors.quandl_error.NotFoundError:
        city_codes_not_working_counter += 1
        if city_codes_not_working_counter == city_codes_len:
          cprint(log_msg.format(yesno=''), 'red')
        continue
    # if we got one working city_code for a city, we skip the rest.
    if working_city_code:
      return row
  return row


def add_housing_data_to_dataframe(dataframe):
  """ Parse every city row from the geocode csv, add the walkscores cells to each. """
  api_count = 0
  for index, row in dataframe.iterrows():
    new_row = add_housing_data_to_row(row)
    dataframe.loc[index] = new_row
    city_code_count = len(row.get('city_code').split('|'))
    api_count += (len(ZILLOW_PRICE_CODES) * city_code_count)
    if api_count % 20 == 0:
      dataframe.to_csv('./primary_sources/zillow/housing.cache')
      log_msg = '### API_COUNT: {} ###'.format(api_count)
      cprint(log_msg, 'yellow')
      time.sleep(8)
  return dataframe


def normalize_city_codes_dataframe(dataframe):
  """ Cleanup the csv from quandl/zillow with city codes so it's ready for joining later. """

  def get_city_and_state_columns(area):
    """ return a pandas series with the city, count and state, this is used by apply. """
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
  dataframe[new_cols] = dataframe['AREA'].apply(get_city_and_state_columns)
  # unfortunately, there are multiple codes for the same city
  # and some of them don't work.
  dataframe['CODE'] = dataframe['CODE'].apply(str)
  dataframe['CODE'] = dataframe['CODE'].str.strip()
  dataframe = dataframe.groupby(['city', 'state'])['CODE'].apply('|'.join)
  dataframe = dataframe.reset_index()
  dataframe = normalize_headers_in_dataframe('zillow_city_codes', dataframe)
  return dataframe


def get_normalized_city_codes_dataframe():
  """ This is the dataframe zillow provides for mapping cities to their city code. """
  dataframe = get_dataframe_from_spreadsheet(CITY_CODES_CSV_FILENAME,
                                             delimiter='|')
  dataframe = normalize_city_codes_dataframe(dataframe)
  return dataframe


def get_final_dataframe():
  """ The main function which returns the final dataframe. """
  city_codes_dataframe = get_normalized_city_codes_dataframe()
  geocodes_dataframe = get_dataframe_from_spreadsheet(
    GEOCODE_FINAL_CSV_FILENAME)
  combined_dataframe = join_on_state_and_city(geocodes_dataframe,
                                              city_codes_dataframe)
  combined_dataframe = add_empty_columns(combined_dataframe,
                                         ZILLOW_PRICE_CODES.keys())
  combined_dataframe = combined_dataframe.reset_index()
  final_dataframe = add_housing_data_to_dataframe(combined_dataframe)
  return final_dataframe


if __name__ == '__main__':
  print('Starting to scrape housing data from quandl.')
  DATAFRAME = get_final_dataframe()
  print('Writing row quantity: ', DATAFRAME['city'].count())
  DATAFRAME.to_csv(ZILLOW_FINAL_CSV_FILENAME, index=False)
  print('Finished writing: ', ZILLOW_FINAL_CSV_FILENAME)
