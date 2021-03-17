#!/usr/bin/env python3
"""Normalize 2020 elections data.

Source: https://github.com/kjhealy/us_elections_2020_csv/

id: Variable length character. Codes are as follows:

For President, Governor, and Senate Races. ONE OF:

   (a) "0", if the row refers to results for a whole state. Identify states
       using fips_char instead.

   (b) A five-digit county FIPS code if the row refers to results for a county.

   (c) A ten-digit FIPS location code for results from a township or similar
       location (the first five characters are this location's county
       FIPS). Note zero padding.

For House races only: A four-digit code consisting of a two-digit State FIPS +
two-digit House District. Note zero padding.  This column should be parsed as
character, not numeric.

"""

import pandas
from file_locations import ELECTIONS_2020_FILENAME, ELECTIONS_FINAL_CSV_FILENAME
from merging_code.utils import get_dataframe_from_spreadsheet
from merging_code.utils import get_logger, write_final_dataframe
from merging_code.normalize_dataframes import normalize_headers_in_dataframes

LOGGER = get_logger('normalize_elections')


def get_final_elections_dataframe():
  """ The main function which returns the normalized elections dataframe. """
  # We need 'id' and 'fips5', which are FIPS character strings, to be read as
  # strings instead of integers.
  elections_2020_dataframe = get_dataframe_from_spreadsheet(
    LOGGER, ELECTIONS_2020_FILENAME, dtype={
      'id': object,
      'fips5': object
    })
  elections_2020_dataframe = normalize_headers_in_dataframes(
    'elections_2020', [elections_2020_dataframe])[0]
  # Filter for only "President" races on the county level.  County FIPS contains 5 digits.
  is_president = elections_2020_dataframe['race'] == 'President'
  is_county = elections_2020_dataframe['id'].str.len() == 5
  is_president_county_race = is_president & is_county
  elections_2020_dataframe = elections_2020_dataframe[is_president_county_race]

  sum_votes = elections_2020_dataframe.groupby('county_fips')['votes'].sum()
  county_total_votes = pandas.DataFrame({'county_total_votes': sum_votes})
  elections_2020_dataframe = elections_2020_dataframe.merge(county_total_votes,
                                                            how='left',
                                                            on='county_fips')

  # Filter for only REP, DEM, LIB parties.
  elections_2020_dataframe = elections_2020_dataframe[
    (elections_2020_dataframe['party'] == 'DEM') |
    (elections_2020_dataframe['party'] == 'REP') |
    (elections_2020_dataframe['party'] == 'LIB')]

  # Calculate the percentage each of REP, DEM, LIB received in the total county vote.
  elections_2020_dataframe['county_vote_percent'] = elections_2020_dataframe[
    'votes'] / elections_2020_dataframe['county_total_votes']

  # Collapse the 3 REP, DEM, LIB rows into one row for each county.
  elections_combined_party_rows_dataframe = elections_2020_dataframe.groupby(
    'county_fips')[[
      'party', 'county_vote_percent'
    ]].apply(lambda x: list(zip(x.party, x.county_vote_percent))).reset_index()

  def party_votes(target_party):

    def helper(tuple_list):
      for elt in tuple_list:
        party, percent = elt
        if party == target_party:
          return percent
      return 0

    return helper

  elections_combined_party_rows_dataframe[
    'county_dem_percent'] = elections_combined_party_rows_dataframe[0].apply(
      party_votes('DEM'))
  elections_combined_party_rows_dataframe[
    'county_rep_percent'] = elections_combined_party_rows_dataframe[0].apply(
      party_votes('REP'))
  elections_combined_party_rows_dataframe[
    'county_lib_percent'] = elections_combined_party_rows_dataframe[0].apply(
      party_votes('LIB'))
  elections_combined_party_rows_dataframe = elections_combined_party_rows_dataframe.drop(
    columns=[0])

  return elections_combined_party_rows_dataframe


if __name__ == '__main__':
  write_final_dataframe(LOGGER,
                        get_final_elections_dataframe,
                        ELECTIONS_FINAL_CSV_FILENAME,
                        index=False)
