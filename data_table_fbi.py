"""
Module for parsing any Fbi related data in data/fbi.
"""
import pandas
from data_table import DataTable


class Fbi(DataTable):
  """ Table of FBI data. """

  @staticmethod
  def read(file_path):
    data = pandas.read_excel(file_path, header=3)

    # Remove empty columns.
    # data.drop(data.columns[[13, 14, 15, 16, 17, 18]], axis=1, inplace=True)

    # Replace the '\n' in header names and make lower_case:
    # "Murder and\nnonnegligent\nmanslaughter" =>
    # "murder and nonnegligent manslaughter"
    def normalize_header(header):
      return header.lower().replace('\n', ' ')

    data = data.rename(columns=normalize_header)

    # Remove integers from 'city' and 'state' column values.  Also make
    # everything lowercase.
    def remove_integers(str_val):
      if isinstance(str_val, str):
        return ''.join([i for i in str_val if not i.isdigit()]).lower()
      return str_val

    def remove_integers_from_row(row):
      return pandas.Series(
        [remove_integers(row['city']),
         remove_integers(row['state'])])

    data[['city', 'state']] = data.apply(remove_integers_from_row, axis=1)

    # Propagate 'state' column.
    state = None
    for i, row in data.iterrows():
      if pandas.notnull(row['state']):
        state = row['state']
      data.at[i, 'state'] = state

    # Normalize all numeric columns by population.
    numeric_columns = data.select_dtypes(
      include=['float64', 'int64']).columns.to_list()
    # Don't normalize 'population' column.
    numeric_columns = [col for col in numeric_columns if col != 'population']

    def normalize_by_pop100k(row):
      # Divide by 100k.
      population = row['population']
      # For every numeric column (excluding 'city' and 'state', which are string
      # columns), normalize to crime per 100k population.
      new_columns = {}
      for column in numeric_columns:
        assert isinstance(row[column], (float, int))
        if population > 0:
          new_columns[column] = row[column] / population * 1e5
        else:
          new_columns[column] = None

      return pandas.Series(new_columns)

    data[numeric_columns] = data.apply(normalize_by_pop100k, axis=1)

    return data

  @staticmethod
  def get_exact_matching_key():
    # By returning `None` as key, we use `index` as key.
    # return None
    return 'index'

  @staticmethod
  def get_state_key():
    return 'state'

  @staticmethod
  def get_city_key():
    return 'city'

  @staticmethod
  def get_population_key():
    return 'population'
