"""
Module for parsing any Walkscore related data in data/walkscore.
"""
import pandas
from merging_code.data_table import DataTable


class Walkscore(DataTable):
  """ Table of Experian data. """

  @staticmethod
  def read(file_path):
    data = pandas.read_csv(file_path)
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
    return None
