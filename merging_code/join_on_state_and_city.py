"""Use fuzzy matching to join DataFrames on 'state' and 'city'."""

from fuzzywuzzy import fuzz


def _cities_from_state(city_index, state):
  """Array of cities from state."""
  return city_index.to_frame().loc[state, :].index.values.tolist()


def fuzzy_match(name, list_names, min_score=0):
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


def _get_row_dataframe(dataframe, key):
  """Get a row of DataFrame as type DataFrame with index 0."""
  # Drop the indices for the columns, so row has index `0`.
  return dataframe.loc[key].to_frame().T.reset_index(drop=True)


def join_on_state_and_city(left_df, right_df, min_score=0):
  """Join two dataframes on 'state' and 'city' columns."""
  # pylint: disable=too-many-locals
  state_city = ['state', 'city']
  # Using index is faster.
  left_df.set_index(state_city, inplace=True)
  right_df.set_index(state_city, inplace=True)
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
  states = left_missing_keys.get_level_values('state').unique().values.tolist()
  common_df.reset_index(inplace=True)
  # Iterate by state.
  for state in states:
    # Create list of cities in dataframes to match against.
    left_cities = _cities_from_state(left_missing_keys, state)
    right_cities = _cities_from_state(right_missing_keys, state)
    for city in left_cities:
      max_city, max_score = fuzzy_match(city, right_cities, min_score=min_score)
      if max_score > 0:
        left_row = _get_row_dataframe(left_df, (state, city))
        right_row = _get_row_dataframe(right_df, (state, max_city))
        merge_rows = left_row.merge(right_row,
                                    how='inner',
                                    left_index=True,
                                    right_index=True)
        # Add `state` and `city`.
        merge_rows.insert(0, 'state', [state])
        merge_rows.insert(1, 'city', [city])
        common_df = common_df.append(merge_rows, ignore_index=True, sort=True)

  common_df.set_index(state_city, inplace=True)
  return common_df
