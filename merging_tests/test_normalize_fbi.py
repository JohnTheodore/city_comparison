""" All the code in merging_code.normalize_fbi should get tested here. """

from merging_code.normalize_fbi import get_final_fbi_dataframe


def test_boulder_area():
  """ We know the fbi property crime for boulder is ~2768, let's test this. """
  dataframe = get_final_fbi_dataframe()
  boulder_property_crime = dataframe.loc[('colorado', 'boulder'),
                                         'property crime']
  assert round(boulder_property_crime, 2) == 3013.2
