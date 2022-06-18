# -*- encoding: UTF-8 -*-

import talib as tl
import pandas as pd
import logging
from strategy import turtle_trade


# 持续上涨（MA30向上）
def check(code_name, data, end_date=None, threshold=30):
    if data.shape[0] < threshold:
        logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold))
        return
    data['ma30'] = pd.Series(tl.MA(data['close'].values, threshold), index=data.index.values)

    begin_date = data.iloc[0].date
    if end_date is not None:
        if end_date < begin_date:  # 该股票在end_date时还未上市
            logging.debug("{}在{}时还未上市".format(code_name, end_date))
            return False

    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]

    data = data.tail(n=threshold)
    ma30 = data['ma30']
    step1 = round(threshold/3)
    step2 = round(threshold*2/3)
    try:
        flag = ma30[0] < ma30[step1] < ma30[step2] < ma30[-1] and ma30[-1] > 1.2*ma30[0]
        return flag
    except Exception as e:
        print(e)
        return False

