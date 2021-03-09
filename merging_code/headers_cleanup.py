"""
Helper functions to cleanup the headers from the various data sources.
This helps make the final product more human readable.
"""

HEADERS_CHANGE = {
  'census_2019': {
    'rename_columns': {
      'state': 'state_fips_part',
      'county': 'county_fips_part',
      'popestimate2019': 'county_population'
    },
    'drop_columns': {
      'sumlev','region','division','stname','ctyname','census2010pop','estimatesbase2010','popestimate2010','popestimate2011','popestimate2012','popestimate2013','popestimate2014','popestimate2015','popestimate2016','popestimate2017','popestimate2018','npopchg_2010','npopchg_2011','npopchg_2012','npopchg_2013','npopchg_2014','npopchg_2015','npopchg_2016','npopchg_2017','npopchg_2018','npopchg_2019','births2010','births2011','births2012','births2013','births2014','births2015','births2016','births2017','births2018','births2019','deaths2010','deaths2011','deaths2012','deaths2013','deaths2014','deaths2015','deaths2016','deaths2017','deaths2018','deaths2019','naturalinc2010','naturalinc2011','naturalinc2012','naturalinc2013','naturalinc2014','naturalinc2015','naturalinc2016','naturalinc2017','naturalinc2018','naturalinc2019','internationalmig2010','internationalmig2011','internationalmig2012','internationalmig2013','internationalmig2014','internationalmig2015','internationalmig2016','internationalmig2017','internationalmig2018','internationalmig2019','domesticmig2010','domesticmig2011','domesticmig2012','domesticmig2013','domesticmig2014','domesticmig2015','domesticmig2016','domesticmig2017','domesticmig2018','domesticmig2019','netmig2010','netmig2011','netmig2012','netmig2013','netmig2014','netmig2015','netmig2016','netmig2017','netmig2018','netmig2019','residual2010','residual2011','residual2012','residual2013','residual2014','residual2015','residual2016','residual2017','residual2018','residual2019','gqestimatesbase2010','gqestimates2010','gqestimates2011','gqestimates2012','gqestimates2013','gqestimates2014','gqestimates2015','gqestimates2016','gqestimates2017','gqestimates2018','gqestimates2019','rbirth2011','rbirth2012','rbirth2013','rbirth2014','rbirth2015','rbirth2016','rbirth2017','rbirth2018','rbirth2019','rdeath2011','rdeath2012','rdeath2013','rdeath2014','rdeath2015','rdeath2016','rdeath2017','rdeath2018','rdeath2019','rnaturalinc2011','rnaturalinc2012','rnaturalinc2013','rnaturalinc2014','rnaturalinc2015','rnaturalinc2016','rnaturalinc2017','rnaturalinc2018','rnaturalinc2019','rinternationalmig2011','rinternationalmig2012','rinternationalmig2013','rinternationalmig2014','rinternationalmig2015','rinternationalmig2016','rinternationalmig2017','rinternationalmig2018','rinternationalmig2019','rdomesticmig2011','rdomesticmig2012','rdomesticmig2013','rdomesticmig2014','rdomesticmig2015','rdomesticmig2016','rdomesticmig2017','rdomesticmig2018','rdomesticmig2019','rnetmig2011','rnetmig2012','rnetmig2013','rnetmig2014','rnetmig2015','rnetmig2016','rnetmig2017','rnetmig2018','rnetmig2019'
    }
  },
  'cdc': {
    'rename_columns': {
      'fips county code': 'county_fips',
      'deaths involving covid-19': 'county_covid19_deaths',
      'deaths from all causes': 'county_all_cause_deaths',
    },
    'drop_columns': [
      'date as of', 'start date', 'end date', 'state', 'county name',
      'urban rural code', 'footnote'
    ]
  },
  'zillow_city_codes': {
    'rename_columns': {
      'code': 'city_code'
    },
    'drop_columns': ['area']
  },
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
      'target geo id2': 'geoid'
    },
    'drop_columns': [
      'density per square mile of land area - housing units',
      'density per square mile of land area - population', 'geographic area',
      'geographic area.1', 'geography', 'housing units', 'id', 'id2',
      'population', 'target geo id'
    ]
  },
  'final_csv': {
    'rename_columns': {},
    'drop_columns': [
      'city_code', 'city_fbi_crime', 'city_walkscore', 'cityexperian_2017',
      'cityzillow', 'geography census_2010', 'geography.1', 'geography.2',
      'latitudezillow', 'longitudezillow', 'rape_legacy', 'reverse_address',
      'reverse_addresszillow', 'state_fbi_crime', 'state_fbi_crime',
      'state_walkscore', 'state_walkscore', 'stateexperian_2017', 'statezillow',
      'target geo id2'
    ]
  }
}
