"""
Helper functions that are used in multiple different classes or files.
"""

import json
import os


def read_json_file(filename):
  """ Read json file, loads it, and return the dict """
  if os.path.isfile(filename):
    with open(filename) as file_handler:
      text = file_handler.read()
      return json.loads(text)
  return {}


def write_json_file(filename, cached_json):
  """ Take a python dict, write it to a file as json.dumps. """
  with open(filename, 'w') as file_handler:
    file_handler.write(json.dumps(cached_json))
