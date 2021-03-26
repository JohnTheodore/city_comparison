"""Test percentage of county that voted for Dem, Rep, Lib parties. """

from merging_code.normalize_elections import get_final_elections_dataframe


def test_normalize_elections():
  dataframe = get_final_elections_dataframe()
  # Usually we keep the FIPS as integer, but for elections data we read it as a
  # string.
  boulder_row = dataframe.loc[dataframe['county_fips'] == '08013']
  print(boulder_row)
  county_dem_percent = boulder_row.get('county_dem_percent')
  assert round(float(county_dem_percent), 2) == 0.77
  county_rep_percent = boulder_row.get('county_rep_percent')
  assert round(float(county_rep_percent), 2) == 0.21
  county_lib_percent = boulder_row.get('county_lib_percent')
  assert round(float(county_lib_percent), 2) == 0.01
