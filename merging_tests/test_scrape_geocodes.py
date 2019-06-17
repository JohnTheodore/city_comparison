""" All the code in merging_code.scrape_geocodes should get tested here. """

from merging_code.scrape_geocodes import get_final_dataframe
from merging_tests.utils import get_city_state_row


def test_sunnyvale_geo():
  """ We know the lat, long and reverse address for sunnyvale, let's test this. """
  dataframe = get_final_dataframe()
  sunnyvale = get_city_state_row(dataframe, 'sunnyvale', 'california')
  assert len(sunnyvale) == 1
  assert float(sunnyvale.get('latitude')) == 37.36883
  assert float(sunnyvale.get('longitude')) == -122.0363496
  sunnyvale_address = '390 W El Camino Real, Sunnyvale, CA 94087, USA'
  assert sunnyvale.get('reverse_address').iloc[0] == sunnyvale_address
