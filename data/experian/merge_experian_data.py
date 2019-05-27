""" Merge 3 seperate csv files from experian into one combined csv. """

import glob
from pandas import read_csv

COMBINED_CSV_FILENAME = './experian_combined_data.csv'


def get_all_csv_files(directory):
  all_csv_files = glob.glob('{}{}'.format(directory, '*.csv'))
  all_csv_files.remove(COMBINED_CSV_FILENAME)
  return all_csv_files


def get_dataframes_from_csvs(csv_files):
  dataframes = []
  for csv_file in csv_files:
    dataframe = read_csv(csv_file, encoding='ISO-8859-1')
    dataframe._metadata = {'filename': csv_file}
    dataframes.append(dataframe)
  return dataframes


def get_filename_root(dataframe):
  return dataframe._metadata['filename'].split('./')[1].split('.csv')[0]


def normalize_headers(dataframes):
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
  for dataframe in dataframes:
    dataframe['Credit Score'] = dataframe['Credit Score'].astype(float)


def get_combined_dataframe(dataframes):

  def print_dataframe_row_qty(dataframe, file_source):
    print('File: ', file_source, 'Row Quantity: ',
          str(dataframe['City'].count()))

  combined_dataframe = None
  for dataframe in dataframes:
    print_dataframe_row_qty(dataframe, dataframe['State'][0])
    if combined_dataframe is None:
      combined_dataframe = dataframe
      combined_dataframe._metadata['filename'] = COMBINED_CSV_FILENAME
      continue
    merge_on = ['City', 'State', 'Credit Score']
    if 'County' in dataframe:
      merge_on.append('County')
    combined_dataframe = combined_dataframe.merge(dataframe,
                                                  on=merge_on,
                                                  how='outer')
    print_dataframe_row_qty(combined_dataframe, COMBINED_CSV_FILENAME)
  return combined_dataframe


def drop_row_if_cols_empty(dataframe, col_names):
  dataframe.dropna(axis=0, subset=col_names, inplace=True)


# merge_experian_data()
experian_csvs = get_all_csv_files('./')
experian_dataframes = get_dataframes_from_csvs(experian_csvs)
normalize_headers(experian_dataframes)
change_credit_score_values_to_float(experian_dataframes)
combined_dataframe = get_combined_dataframe(experian_dataframes)
# drop any rows with empty values in city/state/credit score
drop_row_if_cols_empty(combined_dataframe, ['City', 'State', 'Credit Score'])
combined_dataframe.to_csv(COMBINED_CSV_FILENAME, index=False)
