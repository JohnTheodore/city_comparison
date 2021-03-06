""" All constants that reference city data files will go in here. """

# ./primary_sources/geocode
GEOCODE_CACHED_JSON_FILENAME = './primary_sources/geocode/geo_data.json'
GEOCODE_FINAL_CSV_FILENAME = './primary_sources/geocode/geocodes_scraped.csv'

# ./primary_sources/census
CENSUS_AREA_2010_CSV_FILENAME = './primary_sources/census/DEC_10_SF1_GCTPH1.US13PR_with_ann.csv'
CENSUS_FINAL_CSV_FILENAME = './primary_sources/census/census_normalized.csv'

# ./primary_sources/experian
EXPERIAN_SOURCE_CSV_DIR = './primary_sources/experian/csv_files/'
EXPERIAN_FINAL_CSV_FILENAME = './primary_sources/experian/experian_normalized.csv'

# ./primary_sources/walkscore
WALKSCORE_CACHED_JSON_FILENAME = './primary_sources/walkscore/walkscore_data.json'
WALKSCORE_FINAL_CSV_FILENAME = './primary_sources/walkscore/walkscores_scraped.csv'

# ./primary_sources/willow
ZILLOW_FINAL_CSV_FILENAME = './primary_sources/zillow/zillow_scraped.csv'
ZILLOW_CACHED_JSON_FILENAME = './primary_sources/zillow/zillow_data.json'
CITY_CODES_CSV_FILENAME = './primary_sources/zillow/zillow_city_codes.csv'

# ./primary_sources/fbi
# pylint: disable=line-too-long
FBI_CRIME_2015_XLS_FILENAME = './primary_sources/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2015.xls'
FBI_CRIME_2016_XLS_FILENAME = './primary_sources/fbi/Table_6_Offenses_Known_to_Law_Enforcement_by_State_by_City_2016.xls'
FBI_CRIME_2017_XLS_FILENAME = './primary_sources/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2017.xls'
FBI_CRIME_2018_XLS_FILENAME = './primary_sources/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2018.xls'
FBI_CRIME_2019_XLS_FILENAME = './primary_sources/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2019.xls'
FBI_CRIME_COMBINED_CSV_FILENAME = './primary_sources/fbi/fbi_normalized.csv'

# ./primary_sources/cdc
CDC_PROVISIONAL_COVID19_DEATHS_2020_FILENAME = './primary_sources/cdc/Provisional_COVID-19_Death_Counts_in_the_United_States_by_County.csv'
# We need the county-level population data from the census in order to normalize
# CDC data over population.  We would prefer to have 2020 data, because the
# covid19 death data is from 2020, but that data hasn't been released yet.
CENSUS_2019_POPULATION_FILENAME = './primary_sources/cdc/co-est2019-alldata.csv'
CDC_FINAL_CSV_FILENAME = './primary_sources/cdc/cdc_normalized.csv'

# ./primary_sources/elections
ELECTIONS_2020_FILENAME = './primary_sources/elections/kjhealy_us_elections_2020.csv'
ELECTIONS_FINAL_CSV_FILENAME = './primary_sources/elections/elections_normalized.csv'

# master compiled csv output
MASTER_CSV_FILENAME = './city_comparison.csv'
