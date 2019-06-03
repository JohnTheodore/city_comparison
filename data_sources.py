""" All constants that reference city data files will go in here. """

# ./data/geocode
GEOCODE_CACHED_JSON_FILENAME = './data/geocode/geo_data.json'
GEOCODE_FINAL_CSV_FILENAME = './data/geocode/cities_geo_lat_long_address.csv'

# ./data/census
CENSUS_POPULATION_2017_CSV_FILENAME = './data/census/PEP_2017_PEPANNRSIP.US12A_with_ann.csv'
CENSUS_AREA_2010_CSV_FILENAME = 'data/census/DEC_10_SF1_GCTPH1.US13PR_with_ann.csv'

# ./data/experian
EXPERIAN_SOURCE_CSV_DIR = './data/experian/csv_files/'
EXPERIAN_FINAL_CSV_FILENAME = './data/experian/experian_combined_data.csv'

# ./data/walkscore
WALKSCORE_CACHED_JSON_FILENAME = './data/walkscore/walkscore_data.json'
WALKSCORE_FINAL_CSV_FILENAME = './data/walkscore/cities_walkscores.csv'

# ./data/fbi
FBI_CRIME_2014_XLS_FILENAME = 'data/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2014.xls'
FBI_CRIME_2015_XLS_FILENAME = 'data/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2015.xls'
FBI_CRIME_2016_XLS_FILENAME = 'data/fbi/Table_6_Offenses_Known_to_Law_Enforcement_by_State_by_City_2016.xls'
FBI_CRIME_2017_XLS_FILENAME = 'data/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2017.xls'
FBI_CRIME_COMBINED_CSV_FILENAME = './data/fbi/combined_fbi_mean.csv'

# main compiled csv output
MASTER_CSV_FILENAME = './city_comparison.csv'
