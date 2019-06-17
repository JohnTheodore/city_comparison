"""
Utilities to join FBI and Census data.
We'll generalize to support joining other data tables in the future.
"""

from abc import ABC
import pandas
from merging_code.merge_dataframes import join_on_state_and_city


class DataTable(ABC):
  """ Data table where each row is statistics for a city. """

  def __init__(self, data=None, file_path=None, header=0):
    """
    Create a DataTable containing rows of city data.

    Args:
      data: (Optional) Pandas dataframe.
      file_path: (Optional String) data file path.
    """
    self._file_path = file_path
    self._header = header
    if data is not None:
      self._data = data
    else:
      assert self._file_path is not None
      self._data = self.read(self._file_path, self._header)

  @property
  def data(self):
    """Data represented as pandas DataFrame."""
    return self._data

  @staticmethod
  def read(file_path, header):
    """ Read data from file and return as pandas DataFrame. """
    return pandas.read_csv(file_path, header=header, encoding='ISO-8859-1')

  def join(self, data_table):
    """Join with another DataTable.

    Dispatches to use either "exact" or "fuzzy" matching based on whether
    DataTables are of the same type.

    Args:
      data_table: DataTable.

    Returns:
      DataTable.
    """
    # return self.join_fuzzy_matching(data_table)
    merged_result = join_on_state_and_city(self.data, data_table.data)
    return self.__class__(merged_result)
