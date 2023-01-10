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
from strage_evaluate import load_result
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

COLUMNS = ['停机坪', '均线多头', '放量上涨', '海龟交易法则', '突破平台', '回踩年线', '无大幅回撤']


def process(end_date=None):
    logging.info("************************ process start ***************************************")
    stocks = process_data()
    # process_model(stocks)
    compute(stocks, end_date)
    # max_probability(df)
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
    # if str(x[0]).startswith('60') or str(x[0]).startswith('00')
    stocks = [tuple(x) for x in subset.values if not str(x[0]).startswith('8')]
    if utils.need_update_data() and update:
        utils.prepare()
        data_fetcher.run(stocks)
        check_exit()

    return stocks


def process_model(stocks, end_date):
    if datetime.datetime.now().weekday() == 0:
        strategies['均线多头'] = keep_increasing.check

    for strategy, strategy_func in strategies.items():
        check(stocks, strategy, strategy_func, end_date)
        time.sleep(2)


def compute(stocks, end_date=None):
    df = None
    for strategy, strategy_func in strategies.items():
        cf = check(stocks, strategy, strategy_func, end_date)
        if len(cf) > 0:
            if df is None:
                df = cf
            else:
                # df = df.append(cf)
                df = df.join(cf, on=['code', 'name'], how='outer', lsuffix='_')
    if end_date is None:
        from datetime import datetime
        end_date = datetime.now().strftime('%Y%m%d')
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = 0
    df = df[COLUMNS]
    df.fillna(0, inplace=True)
    df['score'] = df.sum(axis=1)
    df.sort_values(by=['score', '停机坪', '均线多头', '放量上涨', '海龟交易法则'], inplace=True, ascending=False)
    df = add_evaluate(df)
    max_probability(df)
    df.to_csv(f'result/策略-{end_date}.csv', encoding='gbk', float_format='%.2f')
    return df


def check(stocks, strategy, strategy_func, end_date):
    m_filter = check_enter(end_date=end_date, strategy_fun=strategy_func)
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


def max_probability(df: pd.DataFrame):
    def split_strategy(lines: str):
        x = {}
        for line in lines.splitlines():
            k, msg = tuple(line.split(' -> '))
            if k in x:
                x[k] = x[k] + '\n' + msg
            else:
                x[k] = k + "->" + msg
        return x

    notice = '''放量上涨-停机坪 -> 总数据条数：69, 持有2天：正盈利概率65%, >=3%盈利概率40%, >=5%盈利概率30%, >=7%盈利概率17%
    放量上涨-停机坪 -> 总数据条数：69, 持有3天：正盈利概率56%, >=3%盈利概率34%, >=5%盈利概率33%, >=7%盈利概率26%
    停机坪-回踩年线 -> 总数据条数：21, 持有3天：正盈利概率57%, >=3%盈利概率19%, >=5%盈利概率4%, >=7%盈利概率0%
    停机坪-回踩年线 -> 总数据条数：21, 持有4天：正盈利概率57%, >=3%盈利概率19%, >=5%盈利概率4%, >=7%盈利概率0%
    停机坪-无大幅回撤 -> 总数据条数：314, 持有2天：正盈利概率52%, >=3%盈利概率33%, >=5%盈利概率25%, >=7%盈利概率20%
    停机坪-无大幅回撤 -> 总数据条数：314, 持有3天：正盈利概率53%, >=3%盈利概率36%, >=5%盈利概率29%, >=7%盈利概率22%
    停机坪-无大幅回撤 -> 总数据条数：314, 持有5天：正盈利概率52%, >=3%盈利概率44%, >=5%盈利概率38%, >=7%盈利概率33%
    海龟交易法则-放量上涨-停机坪 -> 总数据条数：33, 持有2天：正盈利概率57%, >=3%盈利概率39%, >=5%盈利概率30%, >=7%盈利概率15%
    海龟交易法则-放量上涨-停机坪 -> 总数据条数：33, 持有3天：正盈利概率51%, >=3%盈利概率36%, >=5%盈利概率33%, >=7%盈利概率21%
    海龟交易法则-停机坪-无大幅回撤 -> 总数据条数：63, 持有2天：正盈利概率52%, >=3%盈利概率30%, >=5%盈利概率26%, >=7%盈利概率23%
    海龟交易法则-停机坪-无大幅回撤 -> 总数据条数：63, 持有3天：正盈利概率55%, >=3%盈利概率39%, >=5%盈利概率26%, >=7%盈利概率23%
    海龟交易法则-停机坪-均线多头 -> 总数据条数：210, 持有2天：正盈利概率52%, >=3%盈利概率36%, >=5%盈利概率28%, >=7%盈利概率23%
    海龟交易法则-停机坪-均线多头 -> 总数据条数：210, 持有4天：正盈利概率50%, >=3%盈利概率40%, >=5%盈利概率32%, >=7%盈利概率27%
    放量上涨-突破平台-停机坪 -> 总数据条数：2, 持有2天：正盈利概率50%, >=3%盈利概率0%, >=5%盈利概率0%, >=7%盈利概率0%
    放量上涨-突破平台-回踩年线 -> 总数据条数：7, 持有2天：正盈利概率57%, >=3%盈利概率42%, >=5%盈利概率14%, >=7%盈利概率0%
    放量上涨-突破平台-回踩年线 -> 总数据条数：7, 持有3天：正盈利概率85%, >=3%盈利概率28%, >=5%盈利概率14%, >=7%盈利概率0%
    放量上涨-突破平台-无大幅回撤 -> 总数据条数：19, 持有2天：正盈利概率57%, >=3%盈利概率42%, >=5%盈利概率42%, >=7%盈利概率31%
    放量上涨-突破平台-无大幅回撤 -> 总数据条数：19, 持有3天：正盈利概率57%, >=3%盈利概率52%, >=5%盈利概率52%, >=7%盈利概率42%
    放量上涨-突破平台-均线多头 -> 总数据条数：83, 持有2天：正盈利概率53%, >=3%盈利概率33%, >=5%盈利概率27%, >=7%盈利概率18%
    放量上涨-突破平台-均线多头 -> 总数据条数：83, 持有3天：正盈利概率51%, >=3%盈利概率36%, >=5%盈利概率31%, >=7%盈利概率24%
    放量上涨-停机坪-均线多头 -> 总数据条数：9, 持有2天：正盈利概率55%, >=3%盈利概率44%, >=5%盈利概率44%, >=7%盈利概率22%
    突破平台-停机坪-无大幅回撤 -> 总数据条数：27, 持有3天：正盈利概率51%, >=3%盈利概率37%, >=5%盈利概率22%, >=7%盈利概率14%
    停机坪-回踩年线-均线多头 -> 总数据条数：6, 持有2天：正盈利概率66%, >=3%盈利概率66%, >=5%盈利概率33%, >=7%盈利概率16%
    停机坪-回踩年线-均线多头 -> 总数据条数：6, 持有3天：正盈利概率66%, >=3%盈利概率33%, >=5%盈利概率0%, >=7%盈利概率0%
    停机坪-回踩年线-均线多头 -> 总数据条数：6, 持有4天：正盈利概率50%, >=3%盈利概率16%, >=5%盈利概率0%, >=7%盈利概率0%
    停机坪-无大幅回撤-均线多头 -> 总数据条数：124, 持有2天：正盈利概率50%, >=3%盈利概率30%, >=5%盈利概率24%, >=7%盈利概率20%
    停机坪-无大幅回撤-均线多头 -> 总数据条数：124, 持有3天：正盈利概率50%, >=3%盈利概率31%, >=5%盈利概率27%, >=7%盈利概率21%
    回踩年线-无大幅回撤-均线多头 -> 总数据条数：1, 持有5天：正盈利概率100%, >=3%盈利概率0%, >=5%盈利概率0%, >=7%盈利概率0%'''
    x = split_strategy(notice)
    for k, v in x.items():
        if '-' in k:
            compose = [i.strip() for i in k.split('-')]
            r = df
            for s in compose:
                r = r[r[s] == 1]
            if r.shape[0] > 0:
                print('*' * 50)
                print(v)
                print(r)
                r['comment'] = v
        else:
            r = df[df[k.strip()] == 1]
            if r.shape[0] > 0:
                print('*' * 50)
                print(v)
                print(r)
                r['comment'] = v


def add_evaluate(df: pd.DataFrame):
    df_ = df[COLUMNS]
    r = load_result()

    def match(row):
        k = '-'.join(sorted([i for i, j in zip(COLUMNS, row) if j > 0]))
        if k in r:
            return r[k]
        else:
            return [''] * 5

    d_: pd.Series = df_.apply(match, axis=1)
    d_ = d_.tolist()
    df[['1', '2', '3', '4', '5']] = d_
    return df


def config_mns(line: str):
    a = [i.strip() for i in line.splitlines()]
    sh = [i for i in a if i.startswith('6')]
    sz = [i for i in a if i.startswith('0')]
    print(','.join(sh))
    print(','.join(sz))


if __name__ == '__main__':
    # s = process_data(False)
    # compute(s, end_date='20220329')
    # d = pd.read_csv('result/策略-20220406.csv')
    # d['code'] = d['code'].apply(lambda a: '%06d' % a)
    # d.set_index(keys=['code', 'name'], inplace=True)
    # max_probability(d)
    # add_evaluate(d)
    # print(d)
    # d['score'] = d['score'] / 2
    # d.to_csv(f'result/策略-20220329.csv')
    # print(pd.read_hdf('data/000039.h5'))
    import numpy as np
    process('2022-08-05')
