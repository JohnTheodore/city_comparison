def get_nyc_row(dataframe):
  return dataframe.loc[(dataframe['city'] == 'new york') &
                       (dataframe['state'] == 'new york')]
