#!/usr/bin/env python3
""" County population by FIPS from 2019 Census. """

import numpy as np
from file_locations import CENSUS_2019_POPULATION_FILENAME
from merging_code.utils import get_dataframe_from_spreadsheet
from merging_code.utils import get_logger, write_final_dataframe
from merging_code.normalize_dataframes import normalize_headers_in_dataframes

LOGGER = get_logger('county_population')


def get_county_population_dataframe():
  """ The main function which returns the normalized CDC dataframe. """
  census_2019_dataframe = get_dataframe_from_spreadsheet(
    LOGGER, CENSUS_2019_POPULATION_FILENAME)
  census_2019_dataframe = normalize_headers_in_dataframes(
    'census_2019', [census_2019_dataframe])[0]
  # Check that the "_fips" column types are int64.
  assert census_2019_dataframe['state_fips_part'].dtype == np.int64
  assert census_2019_dataframe['county_fips_part'].dtype == np.int64
  # We need to concatenate the "state_fips_part" and "county_fips_part" columns
  # to form a value that matches the CDC's "county_fips".
  census_2019_dataframe['county_fips'] = (
    census_2019_dataframe['state_fips_part'] * 1000 +
    census_2019_dataframe['county_fips_part'])
  # Remove the "_part" columns.
  census_2019_dataframe = census_2019_dataframe.drop(
    columns=['state_fips_part', 'county_fips_part'])

  LOGGER.info('County population 2019 normalized. Total row count: {}'.format(
    str(len(census_2019_dataframe))))
  return census_2019_dataframe
