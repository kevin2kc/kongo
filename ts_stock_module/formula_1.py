import kongo
import numpy as np
import pandas as pd
from pylab import mpl
import math
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


# 根据不同季度情况，获取最近的EPS的函数
def recent_eps(df_eps):

    dict_df = dict(df_eps)

    for k in list(dict_df.keys()):
        if math.isnan((dict_df[k])):
            del dict_df[k]
            continue

    sort_dict = dict(sorted(dict_df.items(), key=lambda x: x[0], reverse=True))

    for key in sort_dict:

        if key.endswith("1231"):
            return sort_dict[key]
        elif key.endswith("0930"):
            return sort_dict[key] * 4/3
        elif key.endswith("0630"):
            return sort_dict[key] * 2
        elif key.endswith("0331"):
            return sort_dict[key] * 4
        else:
            return np.nan


# 获得回报率pivot table，年回报率，年波动率
# 可以转换为周数据或者月数据
def getstockdata(st_date, ed_date, ts_codes="", index_codes="", freq="D"):

    # ts_code 拆分成组
    if isinstance(ts_codes, str):
        ts_codes = ts_codes.split(',')
    if isinstance(index_codes, str):
        index_codes = index_codes.split(',')

    if ts_codes != [] and index_codes != []:
        total_codes = ts_codes + index_codes
    elif ts_codes is []:
        total_codes = index_codes
    elif index_codes is []:
        total_codes = ts_codes
    else:
        return pd.DataFrame(), None, None

    return_mean, return_sum, return_volatility = [], [], []

    stock_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, ts_codes, method='stock', adj='qfq')
    index_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, index_codes, method='index')

    if not index_price.empty and not stock_price.empty:
        new_price = pd.concat([stock_price[["trade_date", "ts_code", "close"]],
                               index_price[["trade_date", "ts_code", "close"]]], ignore_index=True)
    elif index_price.empty:
        new_price = stock_price
    elif stock_price.empty:
        new_price = index_price
    else:
        return stock_price, return_mean, return_sum, return_volatility

    new_price.set_index('trade_date', inplace=True)
    new_price.index = pd.DatetimeIndex(new_price.index)

    stock_pivot = new_price[["ts_code", "close"]].\
        pivot_table(index='trade_date', columns='ts_code', values='close').copy()

    for i, ts_code in enumerate(total_codes):

        if ts_code in stock_pivot.columns:
            stock_pivot.loc[:, ts_code] = np.log(stock_pivot.loc[:, ts_code] / stock_pivot.loc[:, ts_code].shift(1))

            return_mean.append(round(pd.Series(stock_pivot[ts_code]).mean() * 250, 6))  # 计算股票的平均年化收益率
            return_volatility.append(round(pd.Series(stock_pivot[ts_code]).std()*np.sqrt(250), 6))  # 计算股票的年化收益率波动率
            return_sum.append(round((stock_pivot[ts_code]).sum(), 6))

    return_mean_2 = dict(zip(total_codes, return_mean))
    return_sum_2 = dict(zip(total_codes, return_sum))
    return_volatility_2 = dict(zip(total_codes, return_volatility))

    stock_pivot.dropna(how='all', inplace=True)

    if freq == "W":
        stock_pivot = pd.DataFrame(stock_pivot.resample('W-FRI').sum())
    elif freq == "M":
        stock_pivot = pd.DataFrame(stock_pivot.resample('M').sum())

    return stock_pivot, return_mean_2, return_sum_2, return_volatility_2


def getstockreturnsum(st_date, ed_date, ts_codes="", index_codes=""):

    # ts_code 拆分成组
    if isinstance(ts_codes, str):
        ts_codes = ts_codes.split(',')
    if isinstance(index_codes, str):
        index_codes = index_codes.split(',')

    if ts_codes != [] and index_codes != []:
        total_codes = ts_codes + index_codes
    elif ts_codes is []:
        total_codes = index_codes
    elif index_codes is []:
        total_codes = ts_codes
    else:
        return None

    return_sum = []

    stock_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, ts_codes, method='stock', adj='qfq')
    index_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, index_codes, method='index')

    if not index_price.empty and not stock_price.empty:
        new_price = pd.concat([stock_price[["trade_date", "ts_code", "close"]],
                               index_price[["trade_date", "ts_code", "close"]]], ignore_index=True)
    elif index_price.empty:
        new_price = stock_price
    elif stock_price.empty:
        new_price = index_price
    else:
        return pd.DataFrame()

    new_price.set_index('trade_date', inplace=True)
    new_price.index = pd.DatetimeIndex(new_price.index)

    stock_pivot = new_price[["ts_code", "close"]].\
        pivot_table(index='trade_date', columns='ts_code', values='close').copy()

    for i, ts_code in enumerate(total_codes):

        if ts_code in stock_pivot.columns:
            stock_pivot.loc[:, ts_code] = np.log(stock_pivot.loc[:, ts_code] / stock_pivot.loc[:, ts_code].shift(1))
            return_sum.append(round((stock_pivot[ts_code]).sum(), 6))  # 计算股票期间收益率

    df = pd.DataFrame({'ts_code': total_codes, 'return': return_sum})

    return df


def getstockreturnmeaneachday(st_date, ed_date, ts_codes=""):

    # ts_code 拆分成组
    if isinstance(ts_codes, str):
        ts_codes = ts_codes.split(',')

    stock_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, ts_codes, method='stock', adj='qfq')
    new_price = stock_price[["trade_date", "ts_code", "close"]]

    new_price.set_index('trade_date', inplace=True)
    new_price.index = pd.DatetimeIndex(new_price.index)

    stock_pivot = new_price[["ts_code", "close"]].\
        pivot_table(index='trade_date', columns='ts_code', values='close').copy()

    for i, ts_code in enumerate(ts_codes):

        if ts_code in stock_pivot.columns:
            stock_pivot.loc[:, ts_code] = np.log(stock_pivot.loc[:, ts_code] / stock_pivot.loc[:, ts_code].shift(1))
            # return_sum.append(round((pd.Series(stock_pivot[ts_code])).sum(), 6)) # 计算股票期间收益率

    stock_pivot.dropna(how='all', inplace=True)
    stock_pivot.loc[:, "mean"] = stock_pivot.mean(axis=1)

    return pd.DataFrame(stock_pivot['mean'])

def getstockreturnsumbyweight(st_date, ed_date, ts_codes, weight):

    weightdict = dict(zip(ts_codes, weight))

    stock_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, ts_codes, method='stock', adj='qfq')
    new_price = stock_price[["trade_date", "ts_code", "close"]]

    new_price.set_index('trade_date', inplace=True)
    new_price.index = pd.DatetimeIndex(new_price.index)

    stock_pivot = new_price[["ts_code", "close"]].\
        pivot_table(index='trade_date', columns='ts_code', values='close').copy()

    for i, ts_code in enumerate(ts_codes):

        if ts_code in stock_pivot.columns:
            stock_pivot.loc[:, ts_code] = (np.log(stock_pivot.loc[:, ts_code] /
                                                  stock_pivot.loc[:, ts_code].shift(1))) * weightdict[ts_code]
            # return_sum.append(round((pd.Series(stock_pivot[ts_code])).sum(), 6)) # 计算股票期间收益率

    stock_pivot.dropna(how='all', inplace=True)
    stock_pivot.loc[:, "weightsum"] = stock_pivot.sum(axis=1)

    return pd.DataFrame(stock_pivot['weightsum'])