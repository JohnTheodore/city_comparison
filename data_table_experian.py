"""
Module for parsing any Experian related data in data/experian.
"""
import pandas
from data_table import DataTable


class Experian(DataTable):
  """ Table of Experian data. """

  @staticmethod
  def read(file_path):
    data = pandas.read_csv(file_path)
    data['State'] = data['State'].str.lower()
    data['City'] = data['City'].str.lower()
    return data

  @staticmethod
  def get_exact_matching_key():
    # By returning `None` as key, we use `index` as key.
    # return None
    return 'index'

  @staticmethod
  def get_state_key():
    return 'State'

  @staticmethod
  def get_city_key():
    return 'City'

  @staticmethod
  def get_population_key():
    return None
