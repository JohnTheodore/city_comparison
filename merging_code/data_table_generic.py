"""
Module for parsing any csv data.
"""
import pandas
from merging_code.data_table import DataTable


class ParseCsvData(DataTable):
  """ Table of csv data. """

  @staticmethod
  def read(file_path):
    data = pandas.read_csv(file_path, encoding='ISO-8859-1')
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
