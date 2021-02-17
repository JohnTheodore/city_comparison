from merging_code.get_city_comparison import CSV_FILES_TO_MERGE
from merging_code.merge_dataframes import get_dataframe_from_merged_table_metadata
from merging_code.utils import remove_substring_from_end_of_string, get_logger
from merging_tests.utils import get_city_state_row


def test_remove_substring_from_end_of_string():
  input_string = 'foo bar baz'
  new_string = remove_substring_from_end_of_string(input_string, [' baz'])
  assert new_string == 'foo bar'
  new_string2 = remove_substring_from_end_of_string(input_string,
                                                    [' baz', ' bar'])
  assert new_string2 == 'foo'


def test_madison_row():
  """ We know the lat, long and reverse address for madison, let's test this. """
  dataframe = get_dataframe_from_merged_table_metadata(get_logger('test'),
                                                       CSV_FILES_TO_MERGE)
  madison = get_city_state_row(dataframe, 'madison', 'wisconsin')
  madison_violent_crime = round(float(madison.get('violent crime')), 2)
  assert madison_violent_crime == 365.1
