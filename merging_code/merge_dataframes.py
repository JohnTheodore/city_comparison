"""Use fuzzy matching to join DataFrames on 'state' and 'city'."""

import pandas
from merging_code.normalize_dataframes import drop_headers, rename_headers


def get_all_states_from_index(state_city_index):
  """ Given index of ('state', 'city'), return set of 'states' in index. """
  return set(
    state_city_index.get_level_values('state').unique().values.tolist())


def get_all_cities_from_state(state_city_index, state):
  """ Returns an array of cities from state. """
  return state_city_index.to_frame().loc[state, :].index.values.tolist()


def prefix_match(name, list_names):
  """ Find prefix match for `name` from `list_names`. """
  for name2 in list_names:
    if (name.startswith(name2) or name2.startswith(name)):
      return name2
  return None


def get_row_with_key(dataframe, key):
  """ Get a row of DataFrame as type DataFrame with index 0. """
  # In order to have pandas return a dataframe all the time, pass a *list* of
  # `key`, instead of key directly.
  row = dataframe.loc[[key]]
  # Drop index containing 'state' and 'city'.
  # We'll add those fields back after joining.
  row = row.reset_index(drop=True)
  return row


def set_index_as_state_and_city(dataframe):
  """ Set index as state and city. """
  state_city = ['state', 'city']
  # Using index is faster.
  dataframe = dataframe.reset_index()
  dataframe = dataframe.set_index(state_city)
  dataframe = dataframe.drop(['index'], axis=1)
  return dataframe


def join_on_state_and_city(logger, left_df, right_df):
  """ Join two dataframes on 'state' and 'city' columns. """
  left_df = set_index_as_state_and_city(left_df)
  right_df = set_index_as_state_and_city(right_df)
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
  states = get_all_states_from_index(
    left_missing_keys) & get_all_states_from_index(right_missing_keys)
  # Reset index to make it easier to append new rows.
  common_df = common_df.reset_index()
  # Iterate by state.
  for state in sorted(states):
    logger.debug('state: {}'.format(state))
    # Create list of cities in dataframes to match against.
    left_cities = get_all_cities_from_state(left_missing_keys, state)
    right_cities = get_all_cities_from_state(right_missing_keys, state)
    for city in left_cities:
      max_city = prefix_match(city, right_cities)
      if max_city is not None:
        # Gets data for (state, city); returns a row DataFrame
        # without the 'state' and 'city' fields.
        left_row = get_row_with_key(left_df, (state, city))
        right_row = get_row_with_key(right_df, (state, max_city))
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


# remove everything below this line, or rewrite it.


def get_dataframe_from_merged_table_metadata(logger, tables_metadata):
  """ Take table metadata, and return a merged panda datatable. """
  combined_table = None
  for table_metadata in tables_metadata:
    if combined_table is None:
      combined_table = get_normalized_data_table(logger, table_metadata)
      continue
    next_data_table = get_normalized_data_table(logger, table_metadata)
    combined_table = join_on_state_and_city(logger, combined_table,
                                            next_data_table)
    logger.info('Dataframe length: {}'.format(str(len(combined_table))))
  drop_headers('final_csv', combined_table)
  rename_headers('final_csv', combined_table)
  return combined_table


def get_combined_dataframe(logger,
                           dataframes,
                           how='outer',
                           merge_on='city',
                           optional_merge_on='county'):
  """ Combined all the CSV files, then return the combined dataframe. """
  combined_dataframe = None
  for dataframe in dataframes:
    if combined_dataframe is None:
      combined_dataframe = dataframe
      continue
    merge_on = ['city', 'state', 'credit score']
    for optional_merge in optional_merge_on:
      if optional_merge in dataframe:
        merge_on.append(optional_merge)
    combined_dataframe = combined_dataframe.merge(dataframe,
                                                  on=merge_on,
                                                  how=how)
    log_msg = 'Row quantity for combined_dataframe: {}'.format(
      str(len(combined_dataframe)))
    logger.debug(log_msg)
  return combined_dataframe


def get_normalized_data_table(logger, table_metadata):
  """ Input a dict with csv filename, suffix if available, the document label,
  and return a data_table. """
  data_table = pandas.read_csv(table_metadata['csv_filename'],
                               header=table_metadata.get('header', 0),
                               encoding='ISO-8859-1')
  drop_headers(table_metadata['document_label'], data_table)
  rename_headers(table_metadata['document_label'], data_table)
  log_msg = 'Normalized document_label: {} Dataframe length: {}'.format(
    table_metadata['document_label'], str(len(data_table)))
  logger.info(log_msg)
  # Deduplicate by ('state', 'city').
  data_table = data_table.drop_duplicates(['state', 'city'])
  return data_table
