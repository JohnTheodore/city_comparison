#!/usr/bin/env python3
""" Normalize the census 2010 geography csv. """

import pandas
from file_locations import CENSUS_AREA_2010_CSV_FILENAME, CENSUS_FINAL_CSV_FILENAME
from merging_code.utils import get_dataframe_from_csv


def normalize_dataframe(dataframe):
  """ Clean up the census 2010 csv. """
  # Parse out 'state' and 'city' field from 'Geography.2' field.  There's a
  # '.2' because multiple fields in the header are called 'Geography'.  We
  # should clean that up sometime.
  if 'Geographic area' in dataframe:

    def parse_city_and_state(row):
      city, state = ['NULL', 'NULL']
      if row['Geographic area'].count(' - ') == 2:
        state, city = row['Geographic area'].lower().split(' - ')[-2:]
        if city.endswith(' city'):
          city = city[:-5]
      return pandas.Series([city, state])

    dataframe[['city', 'state']] = dataframe.apply(parse_city_and_state, axis=1)
    dataframe = dataframe[dataframe.state != "NULL"]

  return dataframe


if __name__ == '__main__':
  CENSUS_2010_DATAFRAME = get_dataframe_from_csv(CENSUS_AREA_2010_CSV_FILENAME,
                                                 header=1)
  CENSUS_2010_DATAFRAME = normalize_dataframe(CENSUS_2010_DATAFRAME)
  CENSUS_2010_DATAFRAME.to_csv(CENSUS_FINAL_CSV_FILENAME, index=False)
