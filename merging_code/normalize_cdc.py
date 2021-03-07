#!/usr/bin/env python3
""" Normalize data from the CDC. """
import pandas

from file_locations import CDC_PROVISIONAL_COVID19_DEATHS_2020_FILENAME, CDC_FINAL_CSV_FILENAME
from merging_code.utils import get_dataframe_from_spreadsheet
from merging_code.utils import get_logger, write_final_dataframe
from merging_code.normalize_dataframes import normalize_headers_in_dataframes

LOGGER = get_logger('normalize_cdc')


def get_final_cdc_dataframe():
  """ The main function which returns the normalized CDC dataframe. """
  cdc_2020_dataframe = get_dataframe_from_spreadsheet(
    LOGGER,
    CDC_PROVISIONAL_COVID19_DEATHS_2020_FILENAME)
  cdc_2020_dataframe = normalize_headers_in_dataframes(
    'cdc', [cdc_2020_dataframe])[0]
  LOGGER.info('CDC 2020 normalized. Total row count: {}'.format(
    str(len(cdc_2020_dataframe))))
  return cdc_2020_dataframe


if __name__ == '__main__':
  write_final_dataframe(LOGGER,
                        get_final_cdc_dataframe,
                        CDC_FINAL_CSV_FILENAME,
                        index=False)
