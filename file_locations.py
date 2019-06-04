""" All constants that reference city data files will go in here. """

# ./primary_sources/geocode
GEOCODE_CACHED_JSON_FILENAME = './primary_sources/geocode/geo_data.json'
GEOCODE_FINAL_CSV_FILENAME = './primary_sources/geocode/geocodes_scraped.csv'

# ./primary_sources/census
CENSUS_POPULATION_2017_CSV_FILENAME = './primary_sources/census/PEP_2017_PEPANNRSIP.US12A_with_ann.csv'
CENSUS_AREA_2010_CSV_FILENAME = './primary_sources/census/DEC_10_SF1_GCTPH1.US13PR_with_ann.csv'

# ./primary_sources/experian
EXPERIAN_SOURCE_CSV_DIR = './primary_sources/experian/csv_files/'
EXPERIAN_FINAL_CSV_FILENAME = './primary_sources/experian/experian_normalized.csv'

# ./primary_sources/walkscore
WALKSCORE_CACHED_JSON_FILENAME = './primary_sources/walkscore/walkscore_data.json'
WALKSCORE_FINAL_CSV_FILENAME = './primary_sources/walkscore/walkscores_scraped.csv'

# ./primary_sources/fbi
# pylint: disable=line-too-long
FBI_CRIME_2014_XLS_FILENAME = './primary_sources/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2014.xls'
FBI_CRIME_2015_XLS_FILENAME = './primary_sources/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2015.xls'
FBI_CRIME_2016_XLS_FILENAME = './primary_sources/fbi/Table_6_Offenses_Known_to_Law_Enforcement_by_State_by_City_2016.xls'
FBI_CRIME_2017_XLS_FILENAME = './primary_sources/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2017.xls'
FBI_CRIME_COMBINED_CSV_FILENAME = './primary_sources/fbi/fbi_normalized.csv'

# master compiled csv output
MASTER_CSV_FILENAME = './city_comparison.csv'
