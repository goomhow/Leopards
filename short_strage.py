from http.cookiejar import CookieJar

import pandas as pd
import requests as req
from strategy.keep_increasing import check as ki_check
import utils
import time
import settings
import talib as tl

settings.init()


def check():
    '''
    通达信尾盘选股法
    14:30开始进行选股，依次按以下步骤执行：
        步骤1： 涨幅 3%-5%
        步骤2： 按量比lb筛选，按量比降序排序，将量比 < 1删除
        步骤3： 将换手率低于5%和高于10%的全部删除
        步骤4： 将结果按流通市值进行排序。并将流通市值低于50亿高于200亿的删除
        步骤5： 按成交量进行操作，将成交量持续放大的个股留下，像台阶一样的更好，将成交量一高一低不稳定的删除
        步骤6： 看个股K线形态，短期看5日/10日/20日均线，搭配60日均线多头向上发散就是最好的形态。
               如果k线形态显示在重要K线下方，一般说明近期该个股的走势是冲高回落，说明上方的套牢盘压力过高，处于成交密集区。这种继续进行剔除。
               把一些K线上方没有任何压力的留下，这样冲高也会更加轻松。
        步骤7： 完成以上步骤后进行精确选取。用分时图来判断强势股的特征，能够跑赢大盘的都属于逆势上涨的，强者恒强的市场，只有选取
                强势股才能把收益最大化，最好能搭配当下热点题材板块。这样支撑就更加有力度。
                把剩下的优质股叠加上证指数的分时图，个股的走势必须是全天在分时图价格上方，这表明个股的涨幅较好，
                市场的气氛充足，在车上的人都能吃到一波盈利，次日的冲高会更加有力度。
        步骤8： 剩下的个股都是非常强势的优选股，根据行情的机会，优势股有时候可能一只都筛选不出来。也很正常，要耐心的持之以恒。
               把剩下的个股看在下午2：30分之后，股价创出当日新高，就是目标个股，当个股回落到均线附近时，不跌破就是最好的入场时机。
               止盈止损的点位也要设置好，做短线的精髓就是快准狠，快进快出，有盈利后行情没有按照预期发展，可以直接出，技术不在于多，
               而在于精。
    '''
    XUE_QIU_URL = 'https://xueqiu.com/service/screener/screen'
    headers = {
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Host': 'xueqiu.com',
        'Referer': 'https://xueqiu.com/hq/screener',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0',
        'X-Requested-With': 'XMLHttpRequest'
    }

    cookies = {
        'bid': 'ce0e1db716e5ae3f04441cebb326ee79_l1779t9i',
        'device_id': '38d9067979c607dbdd6526aa90691a7a',
        'remember': '1',
        'xq_is_login': '1',
        's': 'dc11uia7zl',
        'u': '8338841301',
        'xq_a_token': '01c5b26472260cb6a82187bb3196b2ef06b3f267',
        'xq_r_token': '4364be70192304440149919b82a4419b66843900',
        'xqat': '01c5b26472260cb6a82187bb3196b2ef06b3f267',
        'xq_id_token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjgzMzg4NDEzMDEsImlzcyI6InVjIiwiZXhwIjoxNjUwNjkzNzYwLCJjdG0iOjE2NDg1MzIyNzM3MzgsImNpZCI6ImQ5ZDBuNEFadXAifQ.kK8PggxH0VJdMAWnQJYhLA63VuE6hGYRe2o6D2MxoWWKbjKFaFMyp9ywzwRAbpfOxLKBMx4J7xL0noGxsAJXzUS-mnx83ave-QQ0fESu3LvboyGOZPJey_LrmGwXPbVaHk-obK1E1poS3JvUTvQESZpBS-Q7m_-fZ85Qp0DSwWintJ2fAcG-FFCQNJN7m1C1dA21ykgN3t7NbB42HUPEzE6lfzoIakPiZWMPgyLEyBSQ8qbLu9JGM5UiRNq9VNf8JJ9vjpA3mF_lY99JQNNMDG16XvDa2j2pVG-oSf0MU1I9sXlPgVXlNk0VffVec-MfB58Dg48UmhMYluZAI7K3TQ',
        'acw_tc': '2760779d16485318957215495e4f03bd2a4adcd84464a4f5a1ce44eb8ec327'
    }
    params = dict(
        category='CN', exchange='sh_sz', areacode=None, indcode=None, order_by='symbol', order='desc',
        current=None, pct='3_5', volume_ratio='1_1000', tr='5_10', fmc='4500000000_15000000000',
        size=1000, only_count=0, page=1,
        _=int(time.time() * 1000))
    j = req.get(XUE_QIU_URL, params=params, cookies=cookies, headers=headers)

    from datetime import datetime
    end_date = datetime.now().strftime('%Y%m%d')
    if j.status_code == 200:
        good_stocks = pd.DataFrame(j.json()['data']['list'])
        good_stocks['fmc'] = good_stocks['fmc'] / 1.0e8
        good_stocks['keep_increase'] = [keep_increase(i) for i in good_stocks['symbol']]
        good_stocks.to_csv(f'result/短线策略-{end_date}.csv')
        return good_stocks


def keep_increase(code):
    if code.startswith('SZ30'):
        return None
    data = utils.read_data(code)
    return ki_check('', data, threshold=5) and ki_check('', data, threshold=10) and ki_check('', data, threshold=20)


if __name__ == '__main__':
    data = check()
    print(data)
