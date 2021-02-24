""" All the code in merging_code.scrape_walkscores should get tested here. """

from merging_code.scrape_walkscores import get_final_walkscores_dataframe
from merging_tests.utils import get_city_state_row


def test_palo_alto_scores():
  """ We know the lat, long and reverse address for sunnyvale, let's test this. """
  dataframe = get_final_walkscores_dataframe()
  palo_alto = get_city_state_row(dataframe, 'palo alto', 'california')
  assert int(palo_alto.get('walkscore')) == 44
  assert int(palo_alto.get('bikescore')) == 78
  assert int(palo_alto.get('transitscore')) == 29
