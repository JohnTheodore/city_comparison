#!/usr/bin/env python3
""" Merge all the csv files from experian city credit scores into one combined csv. """

from file_locations import EXPERIAN_FINAL_CSV_FILENAME, EXPERIAN_SOURCE_CSV_DIR
from merging_code.utils import get_dataframe_from_spreadsheet, get_all_filenames_with_extension
from merging_code.utils import normalize_headers_in_dataframes, drop_empty_rows_from_dataframes
from merging_code.utils import get_combined_dataframe, lower_case_dataframes_columns


def get_dataframes_from_csv_filenames(csv_files):
  """ Turn a list of csv filenames into a list of panda dataframe objects. """
  dataframes = []
  for csv_file in csv_files:
    dataframe = get_dataframe_from_spreadsheet(csv_file, sheet_type='csv')
    # pylint: disable=W0212
    dataframe._metadata = {'filename': csv_file}
    dataframes.append(dataframe)
  return dataframes


def get_state_name_from_filename(dataframe):
  """ Helper function to take the filename attribute from the panda dataframe object
  and to strip away """
  # pylint: disable=W0212
  return dataframe._metadata['filename'].split(
    EXPERIAN_SOURCE_CSV_DIR)[1].split('.csv')[0]


def add_missing_state_column_to_dataframes(dataframes):
  """ If 'state' and 'State' are missing, we'll add those columns using the filename. """
  for dataframe in dataframes:
    if 'state' not in dataframe.columns and 'State' not in dataframe.columns:
      state_name = get_state_name_from_filename(dataframe)
      dataframe['state'] = [state_name] * dataframe.shape[0]
  return dataframes


def change_credit_score_values_to_int(dataframes):
  """ Make sure all the credit score cell values are normalized as floats. """
  for dataframe in dataframes:
    dataframe['credit score'] = dataframe['credit score'].astype(int)
  return dataframes


def get_final_dataframe():
  """ Turn all the experian CSVs into dataframes, merge them, return the result. """
  experian_csv_filenames = get_all_filenames_with_extension(
    EXPERIAN_SOURCE_CSV_DIR, 'csv')
  dataframes = get_dataframes_from_csv_filenames(experian_csv_filenames)
  dataframes = add_missing_state_column_to_dataframes(dataframes)
  dataframes = normalize_headers_in_dataframes('experian', dataframes)
  # drop any rows with empty values in city/state/credit score
  dataframes = drop_empty_rows_from_dataframes(
    dataframes, ['city', 'state', 'credit score'])
  dataframes = lower_case_dataframes_columns(dataframes,
                                             ['city', 'state', 'county'])
  dataframes = change_credit_score_values_to_int(dataframes)
  final_combined_dataframe = get_combined_dataframe(
    dataframes,
    how='outer',
    merge_on=['city', 'state', 'credit score'],
    optional_merge_on=['county'])
  return final_combined_dataframe


if __name__ == '__main__':
  get_final_dataframe().to_csv(EXPERIAN_FINAL_CSV_FILENAME, index=False)
