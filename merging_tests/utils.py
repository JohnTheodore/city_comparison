def get_city_state_row(dataframe, city, state):
  return dataframe.loc[(dataframe['city'] == city) &
                       (dataframe['state'] == state)]
