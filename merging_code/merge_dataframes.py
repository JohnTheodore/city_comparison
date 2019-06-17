"""Use fuzzy matching to join DataFrames on 'state' and 'city'."""

from fuzzywuzzy import fuzz


def _state_set(state_city_index):
  """Given index of ('state', 'city'), return set of 'states' in index."""
  return set(
    state_city_index.get_level_values('state').unique().values.tolist())


def _cities_from_state(city_index, state):
  """Array of cities from state."""
  return city_index.to_frame().loc[state, :].index.values.tolist()


def fuzzywuzzy_match(name, list_names, min_score=0):
  """Find closest match for `name` from `list_names`."""
  # -1 score for no match.
  max_score = -1
  # Empty name for no match.
  max_name = ""
  # Iterate over all names in the other table.
  for name2 in list_names:
    # Finding fuzzy match score.
    score = fuzz.ratio(name, name2)
    if (score > min_score) & (score > max_score):
      max_name = name2
      max_score = score
  return (max_name, max_score)


def prefix_match(name, list_names):
  """Find closest match for `name` from `list_names`."""
  for name2 in list_names:
    if (name.startswith(name2) or name2.startswith(name)):
      return name2
  return None


def _get_row_dataframe(dataframe, key):
  """Get a row of DataFrame as type DataFrame with index 0."""
  # row = dataframe.loc[[key]].to_frame().T

  # In order to have pandas return a dataframe all the time, pass a *list* of
  # `key`, instead of key directly.
  row = dataframe.loc[[key]]
  # Drop index containing 'state' and 'city'.
  # We'll add those fields back after joining.
  row = row.reset_index(drop=True)
  # return row.T.reset_index(drop=True)
  return row


def normalize_left_right_indices(left_df, right_df):
  """ Setup the indices for faster performance on the dataframes. """
  state_city = ['state', 'city']
  # Using index is faster.
  left_df = left_df.reset_index()
  right_df = right_df.reset_index()
  left_df = left_df.set_index(state_city)
  right_df = right_df.set_index(state_city)
  left_df = left_df.drop(['index'], axis=1)
  right_df = right_df.drop(['index'], axis=1)
  return (left_df, right_df)


def join_on_state_and_city(left_df, right_df):
  """Join two dataframes on 'state' and 'city' columns."""
  left_df, right_df = normalize_left_right_indices(left_df, right_df)
  # First, join exact.
  common_df = left_df.merge(right_df,
                            how='inner',
                            left_index=True,
                            right_index=True)

  # Figure out which keys are missing.
  left_missing_keys = left_df.index.difference(common_df.index)
  right_missing_keys = right_df.index.difference(common_df.index)

  # Now perform "fuzzy matching" inner join between the left and right keys that
  # don't already have a match.  The `state` *must* match.
  # List of states that are in both left and right sides.
  states = _state_set(left_missing_keys) & _state_set(right_missing_keys)
  # Reset index to make it easier to append new rows.
  common_df = common_df.reset_index()
  # Iterate by state.
  for state in sorted(states):
    print('state: ', state)
    # Create list of cities in dataframes to match against.
    left_cities = _cities_from_state(left_missing_keys, state)
    right_cities = _cities_from_state(right_missing_keys, state)
    for city in left_cities:
      max_city = prefix_match(city, right_cities)
      if max_city is not None:
        # Gets data for (state, city); returns a row DataFrame
        # without the 'state' and 'city' fields.
        left_row = _get_row_dataframe(left_df, (state, city))
        right_row = _get_row_dataframe(right_df, (state, max_city))
        merge_rows = left_row.merge(right_row,
                                    how='inner',
                                    left_index=True,
                                    right_index=True)
        # Add `state` and `city`.
        merge_rows.insert(0, 'state', [state])
        merge_rows.insert(1, 'city', [city])
        assert 'index' not in common_df.columns.values
        common_df = common_df.append(merge_rows, ignore_index=True, sort=True)

  assert 'index' not in common_df.columns.values
  return common_df
