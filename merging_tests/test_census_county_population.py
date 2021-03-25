"""Test getting county population from census data."""

from merging_code.census_county_population import get_county_population_dataframe


def test_census_county_population():
  """We expect the 2019 population for Boulder, CO to be 326196."""
  dataframe = get_county_population_dataframe()
  boulder_county_row = dataframe.loc[dataframe['county_fips'] == 8013]
  boulder_county_population = boulder_county_row.get('county_population')
  assert float(boulder_county_population) == 326196
