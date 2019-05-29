"""
Helper functions to cleanup the headers from the various data sources.
This helps make the final product more human readable.
"""

HEADERS_CHANGE = {
  'census_2010': {
    'rename_columns': {
      'Area in square miles - Land area': 'land area sqmi census_2010',
      'Area in square miles - Total area': 'total area sqmi census_2010',
      'Area in square miles - Water area': 'water area sqmi census_2010',
      'Geographic area.1': 'geography census_2010',
    },
    'drop_columns': [
      'Density per square mile of land area - Housing units',
      'Density per square mile of land area - Population',
      'Geographic area',
      'Geography',
      'Housing units',
      'Id',
      'Id2',
      'Population',
      'Target Geo Id',
    ]
  },
  'census_2017': {
    'rename_columns': {
      'Population Estimate (as of July 1) - 2010': 'population census_2010',
      'Population Estimate (as of July 1) - 2017': 'population census_2017'
    },
    'drop_columns': [
      'April 1, 2010 - Census',
      'April 1, 2010 - Estimates Base',
      'Geography',
      'Id',
      'Id2',
      'Population Estimate (as of July 1) - 2011',
      'Population Estimate (as of July 1) - 2012',
      'Population Estimate (as of July 1) - 2013',
      'Population Estimate (as of July 1) - 2014',
      'Population Estimate (as of July 1) - 2015',
      'Population Estimate (as of July 1) - 2016',
      'Rank',
      'Target Geo Id',
    ]
  },
  'final_csv': {
    'rename_columns': {},
    'drop_columns': [
      'delme',
      'city_fbi_crime',
      'city_walkscore',
      'City',
      'geography census_2010',
      'Geography.1',
      'Geography.2',
      'reverse_address',
      'state_fbi_crime',
      'state_fbi_crime',
      'state_walkscore',
      'State',
      'Target Geo Id2',
    ]
  }
}


def drop_headers(data_source, pandas_dataframe):
  """ Mutate the pandas_dataframe and drop the headers from HEADERS_CHANGE """
  if data_source not in HEADERS_CHANGE:
    return
  for column_name in pandas_dataframe.columns:
    if column_name in HEADERS_CHANGE[data_source]['drop_columns']:
      pandas_dataframe.drop(column_name, axis=1, inplace=True)
  return


def rename_headers(data_source, pandas_dataframe):
  """ Mutate the pandas_dataframe and rename the headers from HEADERS_CHANGE """
  if data_source not in HEADERS_CHANGE:
    return
  pandas_dataframe.rename(columns=HEADERS_CHANGE[data_source]['rename_columns'],
                          inplace=True)
  return
