import matplotlib.pyplot as plt

import kongo
import numpy as np
import pandas as pd
import matplotlib
from pylab import mpl
import statsmodels.api as sm
import datetime
import scipy.optimize as sco

mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


def getstockdata(st_date, ed_date, ts_codes, index_codes="", freq="D"):

    if isinstance(ts_codes, str):
        ts_codes = [ts_codes]
    if isinstance(index_codes, str):
        index_codes = [index_codes]

    if index_codes != [""]:
        total_codes = ts_codes + index_codes
    else:
        total_codes = ts_codes

    return_mean, return_volatility = [], []
    return_mean_2, return_volatility_2 = pd.Series(dtype=float), pd.Series(dtype=float)

    stock_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, ts_codes, method='stock', adj='qfq')
    index_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, index_codes, method='index')

    if not index_price.empty:
        new_price = pd.concat([stock_price[["trade_date", "ts_code", "close"]], index_price[["trade_date", "ts_code", "close"]]], ignore_index=True)
    else:
        new_price = stock_price

    new_price.set_index('trade_date', inplace=True)
    new_price.index = pd.DatetimeIndex(new_price.index)

    stock_pivot = new_price[["ts_code", "close"]].pivot_table(index='trade_date', columns='ts_code', values='close').copy()

    for i, ts_code in enumerate(total_codes):

        stock_pivot.loc[:, ts_code] = np.log(stock_pivot.loc[:, ts_code] / stock_pivot.loc[:, ts_code].shift(1))

        return_mean.append(round(pd.Series(stock_pivot[ts_code]).mean() * 250, 6))  # 计算股票的平均年化收益率
        return_volatility.append(pd.Series(stock_pivot[ts_code]).std()*np.sqrt(250))  # 计算股票的年化收益率波动率

        return_mean_2 = pd.Series(return_mean)
        return_volatility_2 = pd.Series(return_volatility)

    # stock_pivot.drop(columns=total_codes, inplace=True)
    stock_pivot.dropna(inplace=True)

    if freq == "W":
        stock_pivot = pd.DataFrame(stock_pivot.resample('W-FRI').sum())

    return stock_pivot, return_mean_2, return_volatility_2


def drawpic():
    db = kongo.Kongo()
    df_stock = db.getstocktscodebyname(["交通银行"])
    df_index = db.getindextscodebyname(["上证指数", "上证50", "上证180", "沪深300"])

    stock_list = df_stock['ts_code'].tolist()
    index_list = df_index['ts_code'].tolist()

    stock_return, return_mean, return_volatility = getstockdata("20110630", "20210630", stock_list, index_list, method='W')

    plt.Figure(figsize=(10, 10))

    plt.subplot(2, 2, 1)
    plt.scatter(x=stock_return['000001.SH_return'], y=stock_return['601328.SH_return'], c='b', marker='o')
    plt.xticks(fontsize=13)
    plt.xlabel(u'上证指数', fontsize=13)
    plt.yticks(fontsize=13)
    plt.ylabel(u'交通银行', fontsize=13, rotation=90)
    plt.grid()

    plt.subplot(2, 2, 2)
    plt.scatter(x=stock_return['000010.SH_return'], y=stock_return['601328.SH_return'], c='c', marker='o')
    plt.xticks(fontsize=13)
    plt.xlabel(u'上证50', fontsize=13)
    plt.yticks(fontsize=13)
    plt.ylabel(u'交通银行', fontsize=13, rotation=90)
    plt.grid()

    plt.subplot(2, 2, 3)
    plt.scatter(x=stock_return['000016.SH_return'], y=stock_return['601328.SH_return'], c='m', marker='o')
    plt.xticks(fontsize=13)
    plt.xlabel(u'上证180', fontsize=13)
    plt.yticks(fontsize=13)
    plt.ylabel(u'交通银行', fontsize=13, rotation=90)
    plt.grid()

    plt.subplot(2, 2, 4)
    plt.scatter(x=stock_return['399300.SZ_return'], y=stock_return['601328.SH_return'], c='y', marker='o')
    plt.xticks(fontsize=13)
    plt.xlabel(u'沪深300', fontsize=13)
    plt.yticks(fontsize=13)
    plt.ylabel(u'交通银行', fontsize=13, rotation=90)
    plt.grid()

    plt.show()


if __name__ == '__main__':
    db = kongo.Kongo()
    df_stock = db.getstocktscodebyname(["赛升药业"])
    df_index = db.getindextscodebyname(["上证指数", "上证50", "上证180", "沪深300", "创业板指"])

    stock_list = df_stock['ts_code'].tolist()
    index_list = df_index['ts_code'].tolist()

    stock_return, return_mean, return_volatility = getstockdata("20150630", "20210630", stock_list, index_list, freq='W')
    print(stock_return)

    SZ_index = stock_return['000001.SH']
    SZ_index_addcons = sm.add_constant(SZ_index)
    model_JT_SZ = sm.OLS(endog=stock_return['300485.SZ'], exog=SZ_index_addcons)
    result_JT_SZ = model_JT_SZ.fit()
    print(result_JT_SZ.summary())

    SZ50_index = stock_return['000010.SH']
    SZ50_index_addcons = sm.add_constant(SZ50_index)
    model_JT_SZ50 = sm.OLS(endog=stock_return['300485.SZ'], exog=SZ50_index_addcons)
    result_JT_SZ50 = model_JT_SZ50.fit()
    print(result_JT_SZ50.summary())

    SZ180_index = stock_return['000016.SH']
    SZ180_index_addcons = sm.add_constant(SZ180_index)
    model_JT_SZ180 = sm.OLS(endog=stock_return['300485.SZ'], exog=SZ180_index_addcons)
    result_JT_SZ180 = model_JT_SZ180.fit()
    print(result_JT_SZ180.summary())

    HS300_index = stock_return['399300.SZ']
    HS300_index_addcons = sm.add_constant(HS300_index)
    model_JT_HS300 = sm.OLS(endog=stock_return['300485.SZ'], exog=HS300_index_addcons)
    result_JT_HS300 = model_JT_HS300.fit()
    print(result_JT_HS300.summary())

    CY_index = stock_return['399006.SZ']
    CY_index_addcons = sm.add_constant(CY_index)
    model_JT_CY = sm.OLS(endog=stock_return['300485.SZ'], exog=CY_index_addcons)
    result_JT_CY = model_JT_CY.fit()
    print(result_JT_CY.summary())
