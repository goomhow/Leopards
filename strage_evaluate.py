import os
from datetime import datetime, timedelta
import logging
import settings
import pandas as pd
import numpy as np
import utils
import strategy.enter as enter
from strategy import turtle_trade
from strategy import backtrace_ma250
from strategy import breakthrough_platform
from strategy import parking_apron
from strategy import low_backtrace_increase
from strategy import keep_increasing

settings.init()
logging.basicConfig(format='%(asctime)s %(message)s', filename='evaluate.log')
logging.getLogger().setLevel(logging.INFO)
strategies = {
    '海龟交易法则': turtle_trade.check_enter,
    '放量上涨': enter.check_volume,
    '突破平台': breakthrough_platform.check,
    '停机坪': parking_apron.check,
    '回踩年线': backtrace_ma250.check,
    '无大幅回撤': low_backtrace_increase.check,
    '均线多头': keep_increasing.check,

}
subset = pd.read_csv(settings.config['stocks_file'], dtype={
    'code': str,
    'name': str,
    'nmc': float
})


def process_result(strategy_name, df=None):
    p = f'result/{strategy_name}-evaluate.csv'
    if df is None:
        df = pd.read_csv(p)
        df.sort_values(by='date', inplace=True)
        df.set_index(keys=['date', 'code_name'], inplace=True)
        return df[['1', '2', '3', '4', '5']]
    else:
        df = df[['1', '2', '3', '4', '5']]
        if os.path.exists(p):
            bak_path = p + '.' + str(int(datetime.now().timestamp()))
            df.to_csv(bak_path)
            old = process_result(strategy_name)
            try:
                old.append(df).to_csv(p)
                os.remove(bak_path)
            except Exception as e:
                logging.error(f"拼接{p}失败")
                logging.error(e)
        else:
            df.to_csv(p)


def evaluate(strategy_name, strategy_func, start, end, dt):
    start_time = datetime.now()
    logging.info('*' * 50)
    logging.info(f"开始评价{strategy_name}模型")
    r = []
    if os.path.exists(f'result/{strategy_name}-evaluate.csv'):
        f_df: pd.DataFrame = process_result(strategy_name=strategy_name)
        max_date = np.max(f_df.index)[0]
        if max_date < end:
            date60 = (datetime.strptime(end, '%Y-%m-%d') - timedelta(days=60)).strftime('%Y-%m-%d')
            if date60 < start:
                date60 = start
            if date60 > max_date:
                date60 = max_date

            days = dt['平安银行'][date60:end].iloc[1:].index
            for day in days:
                d_start = datetime.now()
                logging.info(f'{strategy_name} 开始处理{day}数据')
                for code_name, data in dt.items():
                    if strategy_func(code_name=code_name, data=data, end_date=day):
                        ef: pd.DataFrame = data[day:]
                        ef = ef.iloc[1:6]
                        try:
                            chg = (ef['close'] - ef['open'][0]) / ef['open'][0] * 100
                            r.append([day, code_name] + [i for i in chg])
                        except Exception as e:
                            print(e)

                d_end = datetime.now()
                logging.info(f'{strategy_name}完成处理{day}数据，耗时{d_end - d_start}s')
            if len(r) == 0:
                return
            r = pd.DataFrame(r, columns=['date', 'code_name', '1', '2', '3', '4', '5'])
            r.set_index(keys=['date', 'code_name'], inplace=True)
            r = r[max_date:]
            r = f_df.append(r)
        else:
            r = f_df
    else:
        days = dt['平安银行'][start:end].iloc[1:].index
        for day in days:
            d_start = datetime.now()
            logging.info(f'{strategy_name} 开始处理{day}数据')
            for code_name, data in dt.items():
                if strategy_func(code_name=code_name, data=data, end_date=day):
                    ef: pd.DataFrame = data[day:]
                    ef = ef.iloc[1:6]
                    try:
                        chg = (ef['close'] - ef['open'][0]) / ef['open'][0] * 100
                        r.append([day, code_name] + [i for i in chg])
                    except Exception as e:
                        print(e)

            d_end = datetime.now()
            logging.info(f'{strategy_name}完成处理{day}数据，耗时{d_end - d_start}s')
        if len(r) == 0:
            return
        r = pd.DataFrame(r, columns=['date', 'code_name', '1', '2', '3', '4', '5'])
        r.set_index(keys=['date', 'code_name'], inplace=True)
    total_size = r.shape[0]
    r['strategy_name'] = strategy_name
    process_result(strategy_name, r)
    rt = []
    for col in ['1', '2', '3', '4', '5']:
        n0 = int(r[r[col] > 0].shape[0] / total_size * 100)
        n3 = int(r[r[col] > 3].shape[0] / total_size * 100)
        n5 = int(r[r[col] > 5].shape[0] / total_size * 100)
        n7 = int(r[r[col] > 7].shape[0] / total_size * 100)
        rt.append(' - '.join(['%.2f' % t for t in [n0, n3, n5, n7]]))
        logging.info(f'{strategy_name} -> 持有{col}天：正盈利概率{n0}%, >=3%盈利概率{n3}%, >=5%盈利概率{n5}%, >=7%盈利概率{n7}%')
    end_time = datetime.now()
    logging.info(f"完成评价{strategy_name}模型，耗时{end_time - start_time}s")
    logging.info('*' * 50)
    return rt


def all_strategy_evaluate(start='2020-01-01', end='2022-03-20'):
    datas = {x[1]: utils.read_data(x[0]) for x in subset.values if
             str(x[0]).startswith('60') or str(x[0]).startswith('00')}
    evaluate_result = load_result()
    for n, f in strategies.items():
        rate = evaluate(n, f, start, end, datas)
        evaluate_result[n] = rate
    save_evaluate(evaluate_result)


'''
放量上涨-停机坪 -> 总数据条数：69, 持有2天：正盈利概率65%, >=3%盈利概率40%, >=5%盈利概率30%, >=7%盈利概率17%
放量上涨-停机坪 -> 总数据条数：69, 持有3天：正盈利概率56%, >=3%盈利概率34%, >=5%盈利概率33%, >=7%盈利概率26%
放量上涨-回踩年线 -> 总数据条数：143, 持有1天：正盈利概率52%, >=3%盈利概率24%, >=5%盈利概率14%, >=7%盈利概率7%
停机坪-回踩年线 -> 总数据条数：21, 持有1天：正盈利概率61%, >=3%盈利概率19%, >=5%盈利概率9%, >=7%盈利概率0%
停机坪-回踩年线 -> 总数据条数：21, 持有3天：正盈利概率57%, >=3%盈利概率19%, >=5%盈利概率4%, >=7%盈利概率0%
停机坪-回踩年线 -> 总数据条数：21, 持有4天：正盈利概率57%, >=3%盈利概率19%, >=5%盈利概率4%, >=7%盈利概率0%
停机坪-无大幅回撤 -> 总数据条数：314, 持有1天：正盈利概率52%, >=3%盈利概率29%, >=5%盈利概率21%, >=7%盈利概率10%
停机坪-无大幅回撤 -> 总数据条数：314, 持有2天：正盈利概率52%, >=3%盈利概率33%, >=5%盈利概率25%, >=7%盈利概率20%
停机坪-无大幅回撤 -> 总数据条数：314, 持有3天：正盈利概率53%, >=3%盈利概率36%, >=5%盈利概率29%, >=7%盈利概率22%
停机坪-无大幅回撤 -> 总数据条数：314, 持有5天：正盈利概率52%, >=3%盈利概率44%, >=5%盈利概率38%, >=7%盈利概率33%
回踩年线-无大幅回撤 -> 总数据条数：1, 持有5天：正盈利概率100%, >=3%盈利概率0%, >=5%盈利概率0%, >=7%盈利概率0%
'''


def all_tow_module_evaluate(start=None, end=None, th=49):
    def two_module_evaluate(strategy1, strategy2):
        strategy_name = f'{strategy1}-{strategy2}'
        f1 = process_result(strategy1)
        f1 = f1[start:end]
        f2 = process_result(strategy2)

        f2 = f2[start:end]

        df = f1.join(f2, how='inner', lsuffix='_')
        df = df[['1', '2', '3', '4', '5']]
        total_size = df.shape[0]
        if total_size == 0:
            logging.info(f'{strategy_name} -> 无重合结果')
            return
        rt = []
        for col in ['1', '2', '3', '4', '5']:
            n0 = int(df[df[col] > 0].shape[0] / total_size * 100)
            n3 = int(df[df[col] > 3].shape[0] / total_size * 100)
            n5 = int(df[df[col] > 5].shape[0] / total_size * 100)
            n7 = int(df[df[col] > 7].shape[0] / total_size * 100)
            rt.append(' - '.join(['%.2f' % t for t in [n0, n3, n5, n7]]))
            if max([n0, n3, n5, n7]) > th:
                print(
                    f'{strategy_name} -> 总数据条数：{total_size}, 持有{col}天：正盈利概率{n0}%, >=3%盈利概率{n3}%, >=5%盈利概率{n5}%, >=7%盈利概率{n7}%')
        df['strategy_name'] = strategy_name
        process_result(strategy_name, df)
        return rt

    print('*' * 50)
    keys = list(strategies.keys())
    r = load_result()
    for i in range(0, len(keys)):
        for j in range(i + 1, len(keys)):
            _1, _2 = keys[i], keys[j]
            rate = two_module_evaluate(_1, _2)
            key = '-'.join(sorted([_1, _2]))
            r[key] = rate
    save_evaluate(r)


def all_three_module_evaluate(start=None, end=None, th=49):
    def three_module_evaluate(strategy1, strategy2, strategy3):
        strategy_name = f'{strategy1}-{strategy2}-{strategy3}'
        f1 = process_result(strategy1)
        f1 = f1[start:end]
        f2 = process_result(strategy2)
        f2 = f2[start:end]
        f3 = process_result(strategy3)
        f3 = f3[start:end]
        df = f1.join(f2, how='inner', lsuffix='_').join(f3, how='inner', lsuffix='__')
        df = df[['1', '2', '3', '4', '5']]
        total_size = df.shape[0]
        if total_size == 0:
            logging.info(f'{strategy_name} -> 无重合结果')
            return
        rt = []
        for col in ['1', '2', '3', '4', '5']:
            n0 = int(df[df[col] > 0].shape[0] / total_size * 100)
            n3 = int(df[df[col] > 3].shape[0] / total_size * 100)
            n5 = int(df[df[col] > 5].shape[0] / total_size * 100)
            n7 = int(df[df[col] > 7].shape[0] / total_size * 100)
            rt.append(' - '.join(['%.2f' % t for t in [n0, n3, n5, n7]]))
            if max([n0, n3, n5, n7]) > th:
                logging.info(
                    f'{strategy_name} -> 总数据条数：{total_size}, 持有{col}天：正盈利概率{n0}%, >=3%盈利概率{n3}%, >=5%盈利概率{n5}%, >=7%盈利概率{n7}%')
        df['strategy_name'] = strategy_name
        process_result(strategy_name, df)
        return rt

    logging.info('*' * 50)
    keys = list(strategies.keys())
    evaluate_result = load_result()
    for i in range(0, len(keys) - 2):
        for j in range(i + 1, len(keys) - 1):
            for k in range(j + 1, len(keys)):
                _1, _2, _3 = keys[i], keys[j], keys[k]
                rate = three_module_evaluate(_1, _2, _3)
                key = '-'.join(sorted([_1, _2, _3]))
                evaluate_result[key] = rate
    save_evaluate(evaluate_result)


RESULT_FILE = 'evaluate.json'


def save_evaluate(r: dict):
    import json
    with open(RESULT_FILE, 'w') as fp:
        json.dump(r, fp)


def load_result():
    import json
    if not os.path.exists(RESULT_FILE):
        return {}
    with open(RESULT_FILE, 'r') as fp:
        return json.load(fp)


if __name__ == '__main__':
    from work_flow import process_data
    process_data(True)
    all_strategy_evaluate(start='2020-01-01', end='2022-06-17')
    all_tow_module_evaluate(start='2020-01-01', end='2022-06-17', th=0)
    all_three_module_evaluate(start='2020-01-01', end='2022-06-17', th=0)
