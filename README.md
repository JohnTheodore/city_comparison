# Utilities for analyzing and comparing statistics of different cities

## Datasets Included in the main city_comparison csv

- census 2010 city geography
- fbi crime stats (averaged on a per 100k for last 4 years)
- geocode data for each city (lat, long and reverse address)
- credit scores for each city
- walkscores for each city
- zillow rental index per square foot dollar values for each city.

## Compile the CSV from the various primary sources yourself
```
PYTHONPATH=. python merging_code/get_city_comparison.py
```

## Development Setup

```
python3.7 -m virtualenv py37-mac
source py37-mac/bin/activate
pip install -r requirements-dev.txt
```

Run python run_tests_locally.py _before_ you make a pull request.

## Summary for Every Column Header

"aggravated assault"                   # fbi aggravated assault from the last several years, averaged and normalized per capita
"arson"                                # fbi arson from the last several years, averaged and normalized per capita
"bikescore"                            # bike score of the city scraped from walkscore.com
"block_fips"                           # Federal Information Processing Standard (unique code to identify a "block" in a town)
"burglary"                             # fbi burglary from the last several years, averaged and normalized per capita
"city"                                 # the city/town, this is largely what all the other data revolves around
"county_fips"                          # Federal Information Processing Standard (unique code to identify a county)
"county_name"                          # name of the county that the town is in
"geoid"                                # GEOIDs are numeric codes that uniquely identify all geographic areas (from census_2010)
"land area sqmi census_2010"           # 2010 census just the land area of a town (excluding water)
"larceny theft"                        # fbi larceny theft from the last several years, averaged and normalized per capita
"latitude"                             # latitude from google geocodes api, input was town, state
"longitude"                            # latitude from google geocodes api, input was town, state
"motor vehicle theft"                  # fbi motor vehicle theft from the last several years, averaged and normalized per capita
"murder and nonnegligent manslaughter" # murder and manslaughter, normalized
"population"                           # population of the city, taken from the latest FBI crime xls
"population density"                   # population density, using the fbi town latest population, and the 2010 total area
"population_percent_change"            # percent population change averaged over the last 5 years of FBI population change for each town
"property crime"                       # fbi property crime from the last several years, averaged and normalized per capita
"rape"                                 # fbi rape from the last several years, averaged and normalized per capita
"robbery"                              # fbi robbery from the last several years, averaged and normalized per capita
"state"                                # name of the state the town is in
"total area sqmi census_2010"          # physical square mile area of the city, from the 2010 census
"transitscore"                         # transit score of the city scraped from walkscore.com
"violent crime"                        # fbi violent crime from the last several years, averaged and normalized per capita
"walkscore"                            # walk score of the city scraped from walkscore.com
"water area sqmi census_2010"          # amount of water in square miles for the town, from 2010 census
"reverse_address"                      # reverse address returned from the google geocode API from the city/state string input
"ZRIFAH"                               # Zillow Rental Index Per Square Foot for the town

## Badges

|||
| ------ | ------ |
| travis-ci | [![Build Status](https://travis-ci.org/JohnTheodore/city_comparison.svg?branch=master)](https://travis-ci.org/JohnTheodore/city_comparison) |
|codeclimate|[![Maintainability](https://api.codeclimate.com/v1/badges/197b6ac7279063135428/maintainability)](https://codeclimate.com/github/JohnTheodore/city_comparison/maintainability)|
|codecov|[![codecov](https://codecov.io/gh/JohnTheodore/city_comparison/branch/master/graph/badge.svg)](https://codecov.io/gh/JohnTheodore/city_comparison)|
|gitter|[![Join the chat at https://gitter.im/city_comparison/](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/city_comparison/)|
|requirements|[![Requirements Status](https://requires.io/github/JohnTheodore/city_comparison/requirements.svg?branch=master)](https://requires.io/github/JohnTheodore/city_comparison/requirements/?branch=master)|
