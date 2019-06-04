"""
Helper functions to cleanup the headers from the various data sources.
This helps make the final product more human readable.
"""

HEADERS_CHANGE = {
  'fbi_2014': {
    'rename_columns': {
      'rape (revised definition)1': 'rape',
      'rape (legacy definition)2': 'rape legacy',
      'larceny- theft': 'larceny theft',
      'arson3': 'arson',
    },
    'drop_columns': [],
  },
  'fbi_2015': {
    'rename_columns': {
      'rape (revised definition)1': 'rape',
      'rape (legacy definition)2': 'rape legacy',
      'larceny- theft': 'larceny theft',
      'arson3': 'arson',
    },
    'drop_columns': [],
  },
  'fbi_2016': {
    'rename_columns': {
      'rape (revised definition)1': 'rape',
      'rape (legacy definition)2': 'rape legacy',
      'larceny- theft': 'larceny theft',
      'arson3': 'arson',
    },
    'drop_columns': [],
  },
  'fbi_2017': {
    'rename_columns': {
      'rape1': 'rape',
      'larceny- theft': 'larceny theft',
      'arson2': 'arson',
    },
    'drop_columns': [
      'unnamed: 13',
      'unnamed: 14',
      'unnamed: 15',
      'unnamed: 16',
      'unnamed: 17',
      'unnamed: 18',
    ]
  },
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
  'final_csv': {
    'rename_columns': {},
    'drop_columns': [
      'cityexperian_2017',
      'city_fbi_crime',
      'city_walkscore',
      'City',
      'geography census_2010',
      'Geography.1',
      'Geography.2',
      'reverse_address',
      'rape_legacy',
      'stateexperian_2017',
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
