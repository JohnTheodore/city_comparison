"""
Tests that every column in the city_comparison.csv file has a corresponding documenation string in the README.md
"""

from file_locations import MASTER_CSV_FILENAME
from merging_code.utils import get_dataframe_from_spreadsheet, get_logger


def test_for_docs_on_every_column_header():
  dataframe = get_dataframe_from_spreadsheet(get_logger('test'),
                                             MASTER_CSV_FILENAME)
  column_headers = list(dataframe.columns)
  missing_header_docs = []
  with open('README.md') as readme_file:
    readme = readme_file.read()
  for column_header in column_headers:
    column_header_doc = '"%s"' % column_header
    if column_header_doc not in readme:
      print('Missing column_header docs in README.md for %s' %
            column_header_doc)
      missing_header_docs.append(column_header_doc)
  assert len(missing_header_docs) == 0
