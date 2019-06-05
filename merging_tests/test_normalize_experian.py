""" All the code in merging_code.normalize_experian should get tested here. """

from merging_tests.utils import get_nyc_row
from merging_code.normalize_experian import get_final_dataframe


def test_nyc_area():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  dataframe = get_final_dataframe()
  nyc_row = get_nyc_row(dataframe)
  assert float(nyc_row.get('Credit Score')) == 706.0
