"""
Tests as a sanity check to make sure the data matches what we expect.
"""

from headers_cleanup import HEADERS_CHANGE
from pandas import read_csv

CC = read_csv('city_comparison.csv', encoding='ISO-8859-1')


def test_nyc_area():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  nyc_row = CC.loc[(CC['city'] == 'new york') & (CC['state'] == 'new york')]
  land_area_key = HEADERS_CHANGE['census_2010']['rename_columns'][
    'Area in square miles - Land area']
  nyc_area = nyc_row.get(land_area_key)
  assert float(nyc_area) == 302.64


def test_nyc_credit_score():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  nyc_row = CC.loc[(CC['city'] == 'new york') & (CC['state'] == 'new york')]
  assert float(nyc_row.get('Credit Score')) == 706.0


# def test_nyc_walkscores():
#   """ We know the 2010 census area for nyc is 302.64, let's test this. """
#   nyc_row = CC.loc[(CC['city'] == 'new york') & (CC['state'] == 'new york')]
#   assert float(nyc_row.get('walkscore')) == 98.0
#   assert float(nyc_row.get('bikescore')) == 90.0
#   assert float(nyc_row.get('transitscore')) == 100.0
