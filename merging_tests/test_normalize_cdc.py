"""Test CDC all_cause_death data."""

import math
from merging_tests.utils import get_city_state_row
from merging_code.normalize_cdc import get_final_cdc_dataframe


def test_normalize_cdc():
  """Test expected deaths in Longmont in Boulder County Colorado."""
  # We expect the Boulder county all cause death per 100k to round down to 8951.
  dataframe = get_final_cdc_dataframe()
  boulder_row = dataframe.loc[dataframe['county_fips'] == 8013]
  county_all_cause_deaths_per_100k = boulder_row.get('county_2020_all_cause_deaths_per_100k')
  assert math.floor(float(county_all_cause_deaths_per_100k)) == 8951
  county_covid19_deaths_per_100k = boulder_row.get('county_covid19_deaths_per_100k')
  assert math.floor(float(county_covid19_deaths_per_100k)) == 916
