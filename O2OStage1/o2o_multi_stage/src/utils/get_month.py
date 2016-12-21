import numpy as np

def get_month(date_str):
    date_int = int(date_str)
    year = date_int / 10000
    month = date_int / 100 % 100
    date = date_int % 100
    return int((np.datetime64('%d-%02d' % (year, month)) - np.datetime64('2016-01')) / np.timedelta64(1, 'M') + 1)
