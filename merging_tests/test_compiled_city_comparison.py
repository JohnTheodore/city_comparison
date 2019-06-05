""" All the code in merging_code.compile_city_comparison should get tested here. """

from merging_code.compile_city_comparison import get_final_dataframe
from merging_tests.utils import get_city_state_row


def test_madison_row():
  """ We know the lat, long and reverse address for madison, let's test this. """
  dataframe = get_final_dataframe()
  madison = get_city_state_row(dataframe, 'madison', 'wisconsin')
  madison_violent_crime = round(float(madison.get('violent crime')), 2)
  assert madison_violent_crime == 351.57
