""" All the code in merging_code.normalize_census should get tested here. """

from merging_tests.utils import get_nyc_row
from merging_code.normalize_census import get_final_dataframe


def test_nyc_area():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  dataframe = get_final_dataframe()
  nyc_row = get_nyc_row(dataframe)
  nyc_area = nyc_row.get('Area in square miles - Land area')
  assert float(nyc_area) == 302.64
