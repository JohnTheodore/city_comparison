""" Join Census and FBI data into one combined pandas DataFrame. """

import pandas
from data_table_census import Census as census_data_table
from data_table_fbi import Fbi as fbi_data_table
from data_table_experian import Experian as experian_data_table
from headers_cleanup import cleanup_headers


def debug_print_dataframe(data, num_rows=2, debug=False):
  """ If debug enabled, print a few rows from pandas DataFrame. """
  if debug:
    with pandas.option_context('display.max_rows', None, 'display.max_columns',
                               None):
      print(data[:num_rows])


def main():
  """ Join Census data with FBI data and write out CSV. """

  # Set to True to print out 2 rows out of each dataframe.
  debug = False

  # import census data from 2010 (city area) to a panda table
  census_population_2017_table = census_data_table(
    file_path='data/census/PEP_2017_PEPANNRSIP.US12A_with_ann.csv')
  cleanup_headers('census_2017', census_population_2017_table.data)

  print('census_population_2017_table.data:\n',
        len(census_population_2017_table.data))

  # import census data from 2017 (city population) to a panda table
  census_geography_2010_table = census_data_table(
    file_path='data/census/DEC_10_SF1_GCTPH1.US13PR_with_ann.csv')
  # Note, this mutates the panda dataframe headers on census_geography_2010_table.data.columns
  cleanup_headers('census_2010', census_geography_2010_table.data)

  print('census_geography_2010_table.data:\n',
        len(census_geography_2010_table.data))

  # combine census 2010 city area panda table and census 2017 city population panda table
  combined_census_table = (
    census_population_2017_table.join(census_geography_2010_table))
  print('combined_census_table.data:\n', len(combined_census_table.data))
  debug_print_dataframe(combined_census_table.data, debug=debug)

  # import fbi crime data from 201y (by city) to a panda table
  fbi_crime_table = fbi_data_table(
    file_path=
    'data/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2017.xls',
    suffix='_fbi_crime')
  print('fbi_crime_table.data: ', len(fbi_crime_table.data))
  debug_print_dataframe(fbi_crime_table.data, debug=debug)

  # combine the census panda table with the fbi crime panda table
  combined_table = combined_census_table.join(fbi_crime_table)
  print('combined_table.data: ', len(combined_table.data))
  debug_print_dataframe(combined_table.data, debug=debug)

  # import experian credit score data by city (2017) to a panda table
  experian_credit_score_table = experian_data_table(
    file_path='data/experian/experian_combined_data.csv',
    suffix='experian_2017')
  print('experian_credit_score_table.data: ',
        len(experian_credit_score_table.data))
  debug_print_dataframe(experian_credit_score_table.data, debug=debug)

  # combine the experian panda table with the combined census/fbi table
  combined_table = combined_table.join(experian_credit_score_table)
  print('combined_table.data: ', len(combined_table.data))
  debug_print_dataframe(combined_table.data, debug=debug)
  cleanup_headers('final_csv', combined_table.data)

  # Write the combined dataframe table to the final csv file.
  combined_table.data.to_csv('city_comparison.csv')


if __name__ == '__main__':
  main()
