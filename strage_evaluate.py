from datetime import datetime
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
    '均线多头': keep_increasing.check,
    '无大幅回撤': low_backtrace_increase.check,
    '停机坪': parking_apron.check,
    '回踩年线': backtrace_ma250.check,
}
subset = pd.read_csv(settings.config['stocks_file'], dtype={
    'code': str,
    'name': str,
    'nmc': float
})
datas = {x[1]: utils.read_data(x[0]) for x in subset.values if str(x[0]).startswith('60') or str(x[0]).startswith('00')}
# datas = {'平安银行': utils.read_data('000001')}
days = datas['平安银行']['2020-01-01':'2022-03-20'].index


def evaluate(strategy_name, strategy_func):
    start_time = datetime.now()
    logging.info('*'*50)
    logging.info(f"开始评价{strategy_name}模型")
    r = []
    for day in days:
        d_start = datetime.now()
        logging.info(f'{strategy_name} 开始处理{day}数据')
        for code_name, data in datas.items():
            if strategy_func(code_name=code_name, data=data, end_date=day):
                ef: pd.DataFrame = data[day:]
                ef = ef.iloc[1:6]
                chg = (ef['close'] - ef['open'][0]) / ef['open'][0] * 100
                r.append([day, code_name] + [i for i in chg])
        d_end = datetime.now()
        logging.info(f'{strategy_name}完成处理{day}数据，耗时{d_end - d_start}s')
    r = pd.DataFrame(r, columns=['date', 'code_name', '1', '2', '3', '4', '5'])
    total_size = r.shape[0]
    r['strategy_name'] = strategy_name
    for col in ['1', '2', '3', '4', '5']:
        n0 = int(r[r[col] > 0].shape[0] / total_size * 100)
        n3 = int(r[r[col] > 3].shape[0] / total_size * 100)
        n5 = int(r[r[col] > 5].shape[0] / total_size * 100)
        n7 = int(r[r[col] > 7].shape[0] / total_size * 100)
        logging.info(f'{strategy_name} -> 持有{col}天：正盈利概率{n0}%, >=3%盈利概率{n3}%, >=5%盈利概率{n5}%, >=7%盈利概率{n7}%')
    r.to_csv(f'result/{strategy_name}-evaluate.csv')
    end_time = datetime.now()
    logging.info(f"完成评价{strategy_name}模型，耗时{end_time - start_time}s")
    logging.info('*'*50)


if __name__ == '__main__':
    for n, f in strategies.items():
        evaluate(n, f)
