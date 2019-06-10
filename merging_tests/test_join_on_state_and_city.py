import pandas
from merging_code.join_on_state_and_city import fuzzy_match, join_on_state_and_city


def test_fuzzy_match():
  name = 'a'
  list_names = ['ab', 'ba']
  max_name, max_score = fuzzy_match(name, list_names)
  assert max_name == 'ab'
  assert max_score == 67
  list_names.reverse()
  max_name, max_score = fuzzy_match(name, list_names)
  assert max_name == 'ba'
  assert max_score == 67


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
  })
  expected.set_index(['state', 'city'], inplace=True)
  actual = join_on_state_and_city(left_df, right_df)
  assert actual.equals(expected)
