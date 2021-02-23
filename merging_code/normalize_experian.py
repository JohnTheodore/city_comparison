#!/usr/bin/env python3
""" Merge all the csv files from experian city credit scores into one combined csv. """

from file_locations import EXPERIAN_FINAL_CSV_FILENAME, EXPERIAN_SOURCE_CSV_DIR
from merging_code.normalize_dataframes import drop_empty_rows_from_dataframes, normalize_headers_in_dataframes
from merging_code.normalize_dataframes import lower_case_dataframes_columns
from merging_code.utils import get_dataframe_from_spreadsheet, get_all_filenames_with_extension
from merging_code.utils import get_logger, write_final_dataframe
from merging_code.merge_dataframes import get_combined_dataframe

LOGGER = get_logger('normalize_experian')


def get_dict_of_all_experian_dataframes(csv_files):
  """ Turn a list of csv filenames into a list of panda dataframe objects. """
  dataframes = {}
  for csv_file in csv_files:
    dataframe = get_dataframe_from_spreadsheet(LOGGER,
                                               csv_file,
                                               sheet_type='csv')
    dataframes[csv_file] = dataframe
  return dataframes


def get_state_name_from_filename(file_name):
  """ Helper function to take the filename attribute from the panda dataframe object
  and to strip away """
  return file_name.split(EXPERIAN_SOURCE_CSV_DIR)[1].split('.csv')[0]


def add_missing_state_column_to_dataframes(dataframes):
  """ If 'state' and 'State' are missing, we'll add those columns using the filename. """
  for filename, dataframe in dataframes.items():
    if 'state' not in dataframe.columns and 'State' not in dataframe.columns:
      state_name = get_state_name_from_filename(filename)
      dataframe['state'] = [state_name] * dataframe.shape[0]
  return dataframes


def change_credit_score_values_to_int(dataframes):
  """ Make sure all the credit score cell values are normalized as floats. """
  for dataframe in dataframes:
    dataframe['credit score'] = dataframe['credit score'].astype(int)
  return dataframes


def get_final_experian_dataframe():
  """ Turn all the experian CSVs into dataframes, merge them, return the result. """
  experian_csv_filenames = get_all_filenames_with_extension(
    EXPERIAN_SOURCE_CSV_DIR, 'csv')
  dataframes = get_dict_of_all_experian_dataframes(experian_csv_filenames)
  dataframes = add_missing_state_column_to_dataframes(dataframes)
  dataframes = normalize_headers_in_dataframes('experian', dataframes.values())
  # drop any rows with empty values in city/state/credit score
  dataframes = drop_empty_rows_from_dataframes(
    dataframes, ['city', 'state', 'credit score'])
  dataframes = lower_case_dataframes_columns(dataframes,
                                             ['city', 'state', 'county'])
  dataframes = change_credit_score_values_to_int(dataframes)
  # We used to perform `optional_merge_on=['county']`, however 3 of
  # our primary sources, '100_biggest_cities*', '500_cities_best*',
  # '500_cities_worst*', don't have the 'county' column, while the
  # rest of the Experian CSV files do.  So we will drop merging on
  # 'county' for now.
  #
  # 2021-02-23: I put the optional_merge_on back, and decided to comment out the
  # 3 files mentioned above instead.
  final_combined_dataframe = get_combined_dataframe(
    LOGGER,
    dataframes,
    how='outer',
    merge_on=['city', 'state', 'credit score'],
    optional_merge_on=['county'])
  return final_combined_dataframe


if __name__ == '__main__':
  write_final_dataframe(LOGGER,
                        get_final_experian_dataframe,
                        EXPERIAN_FINAL_CSV_FILENAME,
                        index=False)
