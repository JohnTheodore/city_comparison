#!/usr/bin/env python3
""" Normalize the census 2010 geography csv. """
import pandas

from file_locations import CENSUS_AREA_2010_CSV_FILENAME, CENSUS_FINAL_CSV_FILENAME
from merging_code.utils import get_dataframe_from_spreadsheet, remove_substring_from_end_of_string
from merging_code.utils import get_logger, write_final_dataframe
from merging_code.normalize_dataframes import normalize_headers_in_dataframes

LOGGER = get_logger('normalize_census')


def parse_city_state_from_row(row):
  """ Take a row, add the city/state cells to the row if available. """
  city, state = ['NULL', 'NULL']
  if row['Geographic area'].count(' - ') == 2:
    state, city = row['Geographic area'].lower().split(' - ')[-2:]
    city = remove_substring_from_end_of_string(city, [' city', ' cdp'])
  return pandas.Series([city, state])


def add_city_state_to_dataframe(dataframe):
  """ Clean up the census 2010 csv. """
  dataframe[['city', 'state']] = dataframe.apply(parse_city_state_from_row,
                                                 axis=1)
  dataframe = dataframe[dataframe.state != "NULL"]
  return dataframe


def get_final_census_dataframe():
  """ The main function which returns the final dataframe. """
  census_2010_dataframe = get_dataframe_from_spreadsheet(
    LOGGER, CENSUS_AREA_2010_CSV_FILENAME, header=1, encoding='ISO-8859-1')
  census_2010_dataframe = add_city_state_to_dataframe(census_2010_dataframe)
  census_2010_dataframe = normalize_headers_in_dataframes(
    'census_2010', [census_2010_dataframe])[0]
  LOGGER.info('Census 2010 normalized. Total row count: {}'.format(
    str(len(census_2010_dataframe))))
  return census_2010_dataframe


if __name__ == '__main__':
  write_final_dataframe(LOGGER,
                        get_final_census_dataframe,
                        CENSUS_FINAL_CSV_FILENAME,
                        index=False)
