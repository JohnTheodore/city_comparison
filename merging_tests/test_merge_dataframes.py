import pandas
from merging_code.merge_dataframes import prefix_match, join_on_state_and_city
from merging_code.utils import get_logger


def test_prefix_match():
  name = 'a'
  list_names = ['ab', 'ba']
  max_name = prefix_match(name, list_names)
  assert max_name == 'ab'
  list_names.reverse()
  max_name = prefix_match(name, list_names)
  assert max_name == 'ab'


def test_join_on_state_and_city():
  left_df = pandas.DataFrame(
    data={
      'state': ['a', 'a', 'b', 'b'],
      'city': ['1', '2', '3', '4'],
      'data': [1, 2, 3, 4]
    })
  right_df = pandas.DataFrame(
    data={
      'state': ['a', 'a', 'b', 'c'],
      'city': ['1', '3', '4.', '2'],
      'data': [1, 3, 4, 2]
    })
  # State 'a': city '1' exactly matches city '1'.
  # State 'a': city '2' does not match city '3'.
  # State 'b': city '4' matches city '4.'
  # State 'c' is not present in both tables, so does not get matched.
  expected = pandas.DataFrame(data={
    'state': ['a', 'b'],
    'city': ['1', '4'],
    'data_x': [1, 4],
    'data_y': [1, 4]
  }).set_index(['state', 'city'])
  actual = join_on_state_and_city(get_logger('test'), left_df,
                                  right_df).set_index(['state', 'city'])
  assert actual.equals(expected)
