""" All the code in merging_code.normalize_fbi should get tested here. """

from merging_code.normalize_fbi import get_final_dataframe


def test_boulder_area():
  """ We know the fbi property crime for boulder is ~2768, let's test this. """
  dataframe = get_final_dataframe()
  boulder_poperty_crime = dataframe.loc[('colorado',
                                         'boulder'), 'property crime']
  assert round(boulder_poperty_crime, 2) == round(2768.956145, 2)
