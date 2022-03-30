# -*- encoding: UTF-8 -*-

import data_fetcher
import utils
import strategy.enter as enter
from strategy import turtle_trade
from strategy import backtrace_ma250
from strategy import breakthrough_platform
from strategy import parking_apron
from strategy import low_backtrace_increase
from strategy import keep_increasing
import tushare as ts
import push
import logging
import db
import time
import datetime
import urllib
import settings
import pandas as pd

settings.init()
strategies = {
    '海龟交易法则': turtle_trade.check_enter,
    '放量上涨': enter.check_volume,
    '突破平台': breakthrough_platform.check,
    '均线多头': keep_increasing.check,
    '无大幅回撤': low_backtrace_increase.check,
    '停机坪': parking_apron.check,
    '回踩年线': backtrace_ma250.check,
}


def process():
    logging.info("************************ process start ***************************************")
    stocks = process_data()
    # process_model(stocks)
    compute(stocks)
    logging.info("************************ process   end ***************************************")


def process_data(update=True):
    if update:
        try:
            all_data: pd.DataFrame = ts.get_today_all()
            subset: pd.DataFrame = all_data[['code', 'name', 'nmc']]
            subset['code'] = subset['code'].astype('str')
            subset.to_csv(settings.config['stocks_file'], index=False, header=True)
        except urllib.error.URLError as e:
            print(e)
    subset = pd.read_csv(settings.config['stocks_file'], dtype={
        'code': str,
        'name': str,
        'nmc': float
    })
    stocks = [tuple(x) for x in subset.values if str(x[0]).startswith('60') or str(x[0]).startswith('00')]
    if utils.need_update_data() and update:
        utils.prepare()
        data_fetcher.run(stocks)
        check_exit()

    return stocks


def process_model(stocks):
    if datetime.datetime.now().weekday() == 0:
        strategies['均线多头'] = keep_increasing.check

    for strategy, strategy_func in strategies.items():
        check(stocks, strategy, strategy_func)
        time.sleep(2)


def compute(stocks, end_date=None):
    df = None
    for strategy, strategy_func in strategies.items():
        cf = check(stocks, strategy, strategy_func)
        if len(cf) > 0:
            if df is None:
                df = cf
            else:
                # df = df.append(cf)
                df = df.join(cf, on=['code', 'name'], how='outer', lsuffix='_')
    if end_date is None:
        from datetime import datetime
        end_date = datetime.now().strftime('%Y%m%d')
    df = df[['停机坪', '均线多头', '放量上涨', '海龟交易法则', '突破平台', '回踩年线', '无大幅回撤']]
    df.fillna(0, inplace=True)
    df['score'] = df.sum(axis=1)
    df.sort_values(by=['score', '停机坪', '均线多头', '放量上涨', '海龟交易法则'], inplace=True, ascending=False)
    df.to_csv(f'result/策略-{end_date}.csv')
    return df


def check(stocks, strategy, strategy_func):
    end = None
    m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
    results = list(filter(m_filter, stocks))
    msg = '**************"{0}"**************\n{1}\n**************"{0}"**************\n'.format(strategy, results)
    print(msg)
    push.strategy(msg)
    df = pd.DataFrame(data=results, columns=['code', 'name', 'vol'])
    df[strategy] = 1
    df.set_index(keys=['code', 'name'], inplace=True)
    return df[[strategy]]


def check_enter(end_date=None, strategy_fun=enter.check_volume):
    def end_date_filter(code_name):
        data = utils.read_data(code_name)
        if data is None:
            return False
        else:
            return strategy_fun(code_name, data, end_date=end_date)
        # if result:
        #     message = turtle_trade.calculate(code_name, data)
        #     push.strategy("{0} {1}".format(code_name, message))

    return end_date_filter


# 统计数据
def statistics(all_data, stocks):
    limitup = len(all_data.loc[(all_data['changepercent'] >= 9.5)])
    limitdown = len(all_data.loc[(all_data['changepercent'] <= -9.5)])

    up5 = len(all_data.loc[(all_data['changepercent'] >= 5)])
    down5 = len(all_data.loc[(all_data['changepercent'] <= -5)])

    def ma250(stock):
        stock_data = utils.read_data(stock)
        return enter.check_ma(stock, stock_data)

    ma250_count = len(list(filter(ma250, stocks)))

    msg = "涨停数：{}   跌停数：{}\n涨幅大于5%数：{}  跌幅大于5%数：{}\n年线以上个股数量：    {}" \
        .format(limitup, limitdown, up5, down5, ma250_count)
    push.statistics(msg)


def check_exit():
    t_shelve = db.ShelvePersistence()
    file = t_shelve.open()
    for key in file:
        code_name = file[key]['code_name']
        data = utils.read_data(code_name)
        if turtle_trade.check_exit(code_name, data):
            push.strategy("{0} 达到退出条件".format(code_name))
            del file[key]
        elif turtle_trade.check_stop(code_name, data, file[key]):
            push.strategy("{0} 达到止损条件".format(code_name))
            del file[key]

    file.close()


if __name__ == '__main__':
    # s = process_data(False)
    # compute(s, end_date='20220329')
    # d = pd.read_csv('result/策略-20220329.csv')
    # d['code'] = d['code'].apply(lambda a: '%06d' % a)
    # d.set_index(keys=['code', 'name'], inplace=True)
    # d['score'] = d['score'] / 2
    # d.to_csv(f'result/策略-20220329.csv')
    print(pd.read_hdf('data/000039.h5'))
