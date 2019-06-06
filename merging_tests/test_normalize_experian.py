""" All the code in merging_code.normalize_experian should get tested here. """

from merging_tests.utils import get_city_state_row
from merging_code.normalize_experian import get_final_dataframe


def test_nyc_area():
  """ We know the nyc credit score is 706.0 let's test this. """
  dataframe = get_final_dataframe()
  nyc_row = get_city_state_row(dataframe, 'new york', 'new york')
  assert int(nyc_row.get('credit score')) == 706
