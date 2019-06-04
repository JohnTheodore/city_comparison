from merging_code.data_table_census import Census as census_data_table
from merging_code.data_table_fbi import Fbi as fbi_data_table
from merging_code.data_table import FuzzyMatchingKey

import pandas
import unittest


class TestFbi(unittest.TestCase):

  def test_get_exact_matching_key(self):
    self.assertEqual(fbi_data_table.get_exact_matching_key(), 'index')

  def test_get_state_key(self):
    self.assertEqual(fbi_data_table.get_state_key(), 'state')

  def test_get_city_key(self):
    self.assertEqual(fbi_data_table.get_city_key(), 'city')

  def test_compare_keys_equal(self):
    key1 = FuzzyMatchingKey(state='CA', city='Sunnyvale City', population=100)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), 0)

  def test_compare_keys_equal_wrong_population(self):
    key1 = FuzzyMatchingKey(state='CA', city='Sunnyvale City', population=100)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=200)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), 1)

  def test_compare_keys_city_less_than(self):
    key1 = FuzzyMatchingKey(state='CA', city='Mountain View', population=1)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), -1)

  def test_compare_keys_state_less_than(self):
    key1 = FuzzyMatchingKey(state='AL', city='Montgomery', population=1)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), -1)

  def test_compare_keys_population_less_than(self):
    key1 = FuzzyMatchingKey(state='CA', city='Mountain View', population=1)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), -1)

  def test_init_from_data(self):
    # Test initializing an `Fbi` DataTable from pandas dataframe.
    df = pandas.DataFrame(
      {
        'foo': 1,
        'State': 'CA',
        'City': 'Sunnyvale',
        'Population': 100,
        'state': 'ignored',
        'city': 'ignored'
      },
      index=[0])
    fbi_table = fbi_data_table(data=df)
    self.assertTrue(fbi_table.data.equals(df))

  def test_join_exact_matching(self):
    fbi_data1 = pandas.DataFrame(
      {
        'index': ['california_sunnyvale', 'alabama_montgomery'],
        'foo': [1, 2],
        'State': ['CA', 'AL'],
        'City': ['Sunnyvale', 'Montgomery'],
        'Population': [100, 200],
      },
      index=[0, 1])

    fbi_data2 = pandas.DataFrame(
      {
        'index': ['alabama_montgomery', 'california_sunnyvale'],
        'bar': [3, 4],
        'State': ['AL', 'CA'],
        'City': ['Montgomery', 'Sunnyvale'],
        'Population': [200, 100],
      },
      index=[0, 1])

    fbi_table1 = fbi_data_table(data=fbi_data1, suffix='_table1')
    fbi_table2 = fbi_data_table(data=fbi_data2, suffix='_table2')
    # Note that the indices,
    # index=['california_sunnyvale', 'alabama_montgomery']
    # will be ignored.
    expected_data = pandas.DataFrame({
      'foo': [1, 2],
      'bar': [4, 3],
      'State_table1': ['CA', 'AL'],
      'City_table1': ['Sunnyvale', 'Montgomery'],
      'Population_table1': [100, 200],
      'State_table2': ['CA', 'AL'],
      'City_table2': ['Sunnyvale', 'Montgomery'],
      'Population_table2': [100, 200],
      'index': ['california_sunnyvale', 'alabama_montgomery']
    }).sort_index(axis=1)
    actual_data = fbi_table1.join_exact_matching(fbi_table2).data.sort_index(
      axis=1)
    # We sort the pandas DataFrame columns in order to compare
    self.assertTrue(expected_data.equals(actual_data))


class TestCensus(unittest.TestCase):

  def test_get_state_key(self):
    self.assertEqual(census_data_table.get_state_key(), 'state')

  def test_get_city_key(self):
    self.assertEqual(census_data_table.get_city_key(), 'city')


if __name__ == '__main__':
  unittest.main()
