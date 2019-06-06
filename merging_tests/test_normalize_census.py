""" All the code in merging_code.normalize_census should get tested here. """

from merging_tests.utils import get_city_state_row
from merging_code.headers_cleanup import HEADERS_CHANGE
from merging_code.normalize_census import get_final_dataframe


def test_nyc_area():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  dataframe = get_final_dataframe()
  nyc_row = get_city_state_row(dataframe, 'new york', 'new york')
  land_area_key = HEADERS_CHANGE['census_2010']['rename_columns'][
    'area in square miles - land area']
  nyc_area = nyc_row.get(land_area_key)
  assert float(nyc_area) == 302.64
