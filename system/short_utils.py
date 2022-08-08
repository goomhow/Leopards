'''
                  date  open  close  high   low    volume    code  p_change
day
2022-06-15  2022-06-15  4.66   4.67  4.75  4.65  226442.0  002435  0.214592
2022-06-16  2022-06-16  4.69   4.74  4.78  4.68  231670.0  002435  1.498929
2022-06-17  2022-06-17  4.70   4.72  4.77  4.64  192694.0  002435 -0.421941
2022-06-20  2022-06-20  4.75   4.85  4.85  4.70  338939.0  002435  2.754237
2022-06-21  2022-06-21  4.84   4.79  4.86  4.75  255268.0  002435 -1.237113
2022-06-22  2022-06-22  4.80   4.74  4.82  4.73  179358.0  002435 -1.043841
2022-06-23  2022-06-23  4.72   4.77  4.79  4.68  173351.0  002435  0.632911
'''
import pandas as pd
from instance import *


def get_high_123(stock_data: pd.DataFrame, end_date=None, window=StockTime.YEAR):
    df = stock_data[:end_date]
    df: pd.DataFrame = df.tail(window)
    sorted_close: pd.Series = df['close'].sort_values(ascending=True)
    highest = sorted_close.tail(1)
    lowest = sorted_close.head(1)


def get_lower_123(stock_data: pd.DataFrame, end_date=None, window=100):
    pass


def is_platform(stock_data: pd.DataFrame, end_date=None):
    pass


def platform_high_point(stock_data: pd.DataFrame, end_date=None):
    pass


def platform_lower_point(stock_data: pd.DataFrame, end_date=None):
    pass


def nearest_high_point(stock_data: pd.DataFrame, end_date=None):
    pass


def nearest_lower_point(stock_data: pd.DataFrame, end_date=None):
    pass
