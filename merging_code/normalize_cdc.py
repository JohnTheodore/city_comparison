#!/usr/bin/env python3
""" Normalize data from the CDC. """

import numpy as np
from file_locations import CDC_PROVISIONAL_COVID19_DEATHS_2020_FILENAME, CDC_FINAL_CSV_FILENAME
from merging_code.census_county_population import get_county_population_dataframe
from merging_code.utils import get_dataframe_from_spreadsheet
from merging_code.utils import get_logger, write_final_dataframe
from merging_code.merge_dataframes import JoinColumn
from merging_code.normalize_dataframes import normalize_headers_in_dataframes

LOGGER = get_logger('normalize_cdc')


def get_final_cdc_dataframe():
  """ The main function which returns the normalized CDC dataframe. """
  cdc_2020_dataframe = get_dataframe_from_spreadsheet(
    LOGGER, CDC_PROVISIONAL_COVID19_DEATHS_2020_FILENAME)
  cdc_2020_dataframe = normalize_headers_in_dataframes('cdc',
                                                       [cdc_2020_dataframe])[0]

  census_2019_dataframe = get_county_population_dataframe()
  # Check that the "_fips" column types are int64.
  assert cdc_2020_dataframe['county_fips'].dtype == np.int64
  assert census_2019_dataframe['county_fips'].dtype == np.int64

  cdc_2020_dataframe = JoinColumn.join_with_combined_table(
    LOGGER, cdc_2020_dataframe, census_2019_dataframe,
    {'join_column': JoinColumn.COUNTY_FIPS})

  cdc_2020_dataframe['county_2020_all_cause_deaths_per_100k'] = (
    cdc_2020_dataframe['county_all_cause_deaths'] /
    cdc_2020_dataframe['county_population'] * 10e5)

  cdc_2020_dataframe['county_covid19_deaths_per_100k'] = (
    cdc_2020_dataframe['county_covid19_deaths'] /
    cdc_2020_dataframe['county_population'] * 10e5)

  # Drop the columns we used to calculate "_deaths_per_100k".
  cdc_2020_dataframe = cdc_2020_dataframe.drop(
    columns=['county_all_cause_deaths', 'county_covid19_deaths'])

  LOGGER.info('CDC 2020 normalized. Total row count: {}'.format(
    str(len(cdc_2020_dataframe))))
  return cdc_2020_dataframe


if __name__ == '__main__':
  write_final_dataframe(LOGGER,
                        get_final_cdc_dataframe,
                        CDC_FINAL_CSV_FILENAME,
                        index=False)
