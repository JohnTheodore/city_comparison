""" All the code in merging_code.normalize_census should get tested here. """

from file_locations import CENSUS_AREA_2010_CSV_FILENAME
from merging_code.utils import get_dataframe_from_spreadsheet
from merging_tests.utils import get_nyc_row
from merging_code.normalize_census import normalize_dataframe


def test_nyc_area():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  dataframe = get_dataframe_from_spreadsheet(CENSUS_AREA_2010_CSV_FILENAME,
                                             header=1)
  dataframe = normalize_dataframe(dataframe)
  nyc_row = get_nyc_row(dataframe)
  nyc_area = nyc_row.get('Area in square miles - Land area')
  assert float(nyc_area) == 302.64
