""" Credentials that won't get checked in to git. """

import os

WALKSCORE_API_KEY = os.environ.get('WALKSCORE_API_KEY', '')
GEOCODE_API_KEY = os.environ.get('GEOCODE_API_KEY', '')
QUANDL_API_KEY = os.environ.get('QUANDL_API_KEY', '')
