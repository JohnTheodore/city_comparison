"""
Tests as a sanity check to make sure the data matches what we expect.
"""

from file_locations import MASTER_CSV_FILENAME
from merging_code.headers_cleanup import HEADERS_CHANGE
from merging_code.utils import get_dataframe_from_spreadsheet, get_logger
from merging_tests.utils import get_city_state_row

DATAFRAME = get_dataframe_from_spreadsheet(get_logger('test'),
                                           MASTER_CSV_FILENAME)


def test_nyc_area():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  nyc_row = get_city_state_row(DATAFRAME, 'new york', 'new york')
  land_area_key = HEADERS_CHANGE['census_2010']['rename_columns'][
    'area in square miles - land area']
  nyc_area = nyc_row.get(land_area_key)
  assert float(nyc_area) == 302.64


# def test_nyc_credit_score():
#   """ We know the 2010 census area for nyc is 302.64, let's test this. """
#   nyc_row = get_city_state_row(DATAFRAME, 'new york', 'new york')
#   assert int(nyc_row.get('credit score')) == 706


def test_nyc_walkscores():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  nyc_row = get_city_state_row(DATAFRAME, 'new york', 'new york')
  assert float(nyc_row.get('walkscore')) == 98.0
  assert float(nyc_row.get('transitscore')) == 100.0
