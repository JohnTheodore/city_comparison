#!/usr/bin/env python3
""" Merge all the csv files from experian city credit scores into one combined csv. """

import glob
from pandas import read_csv
from data_sources import EXPERIAN_FINAL_CSV_FILENAME, EXPERIAN_SOURCE_CSV_DIR


def get_all_csv_files(directory):
  """ Get the list of all csv filenames (from the repo root). """
  all_csv_files = glob.glob('{}{}'.format(directory, '*.csv'))
  return all_csv_files


def get_dataframes_from_csvs(csv_files):
  """ Turn a list of csv filenames into a list of panda dataframe objects. """
  dataframes = []
  for csv_file in csv_files:
    dataframe = read_csv(csv_file, encoding='ISO-8859-1')
    # pylint: disable=W0212
    dataframe._metadata = {'filename': csv_file}
    dataframes.append(dataframe)
  return dataframes


def get_filename_root(dataframe):
  """ Helper function to take the filename attribute from the panda dataframe object
  and to strip away """
  # pylint: disable=W0212
  return dataframe._metadata['filename'].split(
    EXPERIAN_SOURCE_CSV_DIR)[1].split('.csv')[0]


def normalize_headers(dataframes):
  """ Rename headers, drop headers, normalize like header names.. """
  header_renames = {
    'City ': 'City',
    'city': 'City',
    'VantageScore 3.0 Credit Score': 'Credit Score',
    'Avg VantageScore 3.0': 'Credit Score',
    'Average VantageScore 3.0 Credit Score': 'Credit Score',
    'Avg. VantageScore 3.0': 'Credit Score',
    'Weighted Vantage Score': 'Credit Score',
    'Sum of Adjusted Credit Score': 'Credit Score',
    ' Average VantageScore 3.0 Credit Score': 'Credit Score',
    'Vantage Score': 'Credit Score',
    'County Name': 'County'
  }
  drop_headers = ['Rank', 'Population', 'Unnamed: 5', 'Unnamed: 4']
  for dataframe in dataframes:
    # Rename headers
    for header in dataframe.columns:
      if header in header_renames:
        dataframe.rename(columns={header: header_renames[header]}, inplace=True)
    # Add 'State' column if missing, based on filename.

    if 'State' not in dataframe.columns:
      state_name = get_filename_root(dataframe)
      non_header_row_qty = dataframe['City'].count()
      state_list = [state_name] * non_header_row_qty
      dataframe['State'] = state_list
    # Drop headers
    for drop_header in drop_headers:
      if drop_header in dataframe:
        dataframe.drop(drop_header, 1, inplace=True)


def change_credit_score_values_to_float(dataframes):
  """ Make sure all the credit score cell values are normalized as floats. """
  for dataframe in dataframes:
    dataframe['Credit Score'] = dataframe['Credit Score'].astype(float)


def get_combined_dataframe(dataframes):
  """ Combined all the CSV files, then return the combined dataframe. """

  def print_dataframe_row_qty(dataframe, file_source):
    """ Helper print function, so we know how many rows we're gaining over time. """
    print('File: ', file_source, 'Row Quantity: ',
          str(dataframe['City'].count()))

  combined_dataframe = None
  for dataframe in dataframes:
    print_dataframe_row_qty(dataframe, dataframe['State'][0])
    if combined_dataframe is None:
      combined_dataframe = dataframe
      # pylint: disable=W0212
      combined_dataframe._metadata['filename'] = EXPERIAN_FINAL_CSV_FILENAME
      continue
    merge_on = ['City', 'State', 'Credit Score']
    if 'County' in dataframe:
      merge_on.append('County')
    combined_dataframe = combined_dataframe.merge(dataframe,
                                                  on=merge_on,
                                                  how='outer')
    print_dataframe_row_qty(combined_dataframe, EXPERIAN_FINAL_CSV_FILENAME)
  return combined_dataframe


def drop_rows_missing_required_cells(dataframe, col_names):
  """ If we're missing any required information, we drop the row entirely. """
  dataframe.dropna(axis=0, subset=col_names, inplace=True)


# merge_experian_data()
EXPERIAN_CSV_FILENAMES = get_all_csv_files(EXPERIAN_SOURCE_CSV_DIR)
EXPERIAN_DATAFRAMES = get_dataframes_from_csvs(EXPERIAN_CSV_FILENAMES)
normalize_headers(EXPERIAN_DATAFRAMES)
change_credit_score_values_to_float(EXPERIAN_DATAFRAMES)
FINAL_COMBINED_DATAFRAME = get_combined_dataframe(EXPERIAN_DATAFRAMES)
# drop any rows with empty values in city/state/credit score
drop_rows_missing_required_cells(FINAL_COMBINED_DATAFRAME,
                                 ['City', 'State', 'Credit Score'])
FINAL_COMBINED_DATAFRAME.to_csv(EXPERIAN_FINAL_CSV_FILENAME, index=False)
