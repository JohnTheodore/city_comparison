"""
Helper functions to cleanup the headers from the various data sources.
This helps make the final product more human readable.
"""

HEADERS_CHANGE = {
  'experian': {
    'rename_columns': {
      'city ': 'city',
      'vantagescore 3.0 credit score': 'credit score',
      'avg vantagescore 3.0': 'credit score',
      'average vantagescore 3.0 credit score': 'credit score',
      'avg. vantagescore 3.0': 'credit score',
      'weighted vantage score': 'credit score',
      'sum of adjusted credit score': 'credit score',
      ' average vantagescore 3.0 credit score': 'credit score',
      'vantage score': 'credit score',
      'county name': 'county',
    },
    'drop_columns': ['rank', 'population', 'unnamed: 5', 'unnamed: 4']
  },
  'fbi_2014': {
    'rename_columns': {
      'rape (revised definition)1': 'rape',
      'larceny- theft': 'larceny theft',
      'arson3': 'arson',
    },
    'drop_columns': ['rape (legacy definition)2'],
  },
  'fbi_2015': {
    'rename_columns': {
      'rape (revised definition)1': 'rape',
      'larceny- theft': 'larceny theft',
      'arson3': 'arson',
    },
    'drop_columns': ['rape (legacy definition)2'],
  },
  'fbi_2016': {
    'rename_columns': {
      'rape (revised definition)1': 'rape',
      'larceny- theft': 'larceny theft',
      'arson3': 'arson',
    },
    'drop_columns': ['rape (legacy definition)2'],
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
      'area in square miles - land area': 'land area sqmi census_2010',
      'area in square miles - total area': 'total area sqmi census_2010',
      'area in square miles - water area': 'water area sqmi census_2010',
    },
    'drop_columns': [
      'density per square mile of land area - housing units',
      'density per square mile of land area - population', 'geographic area',
      'geographic area.1', 'geography', 'housing units', 'id', 'id2',
      'population', 'target geo id', 'target geo id2'
    ]
  },
  'final_csv': {
    'rename_columns': {},
    'drop_columns': [
      'city_fbi_crime',
      'city_walkscore',
      'city',
      'geography census_2010',
      'geography.1',
      'geography.2',
      'reverse_address',
      'rape_legacy',
      'state_fbi_crime',
      'state_fbi_crime',
      'state_walkscore',
      'state',
      'target geo id2',
    ]
  }
}
