import kongo
import numpy as np
import pandas as pd
import matplotlib
from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


def getstockindicator(ts_codes, st_date, ed_date):

    if isinstance(ts_codes, str):
        ts_codes = [ts_codes]

    return_mean, return_volatility = [], []
    return_mean_2, return_volatility_2 = pd.Series(dtype=float), pd.Series(dtype=float)

    stock_price = kongo.Kongo().getstockdailyfrommongo(st_date, ed_date, ts_codes, 'qfq')
    # stock_price = stock_price[["trade_date", "close"]].copy()
    stock_price.set_index('trade_date')

    stock_pivot = stock_price[["trade_date", "ts_code", "close"]].pivot_table(index='trade_date', columns='ts_code', values='close').copy()

    for i, ts_code in enumerate(ts_codes):

        stock_pivot.loc[:, ts_code+"_return"] = np.log(stock_pivot.loc[:, ts_code] / stock_pivot.loc[:, ts_code].shift(1))
        stock_pivot.dropna(inplace=True)

        return_mean.append(round(pd.Series(stock_pivot[ts_code+"_return"]).mean() * 252, 6))  # 计算股票的平均年化收益率
        return_volatility.append(pd.Series(stock_pivot[ts_code+"_return"]).std()*np.sqrt(252))  # 计算股票的年化收益率波动率

        return_mean_2 = pd.Series(return_mean)
        return_volatility_2 = pd.Series(return_volatility)

    stock_pivot.drop(columns=ts_codes, inplace=True)
    return stock_pivot, return_mean_2, return_volatility_2

if __name__ == '__main__':
    stock_pivot, return_mean, return_volatility = getstockindicator(["000001.SZ", "600000.SH", "601939.SH"], "20200701", "20210630")
    print(stock_pivot)
    print(return_mean)
    print(return_volatility)

    x = np.random.random((len(return_mean.index)))
    w = x / np.sum(x)
    print(w)

    return_cov = stock_pivot.cov()*252
    print(return_cov)

    return_corr = stock_pivot.corr()
    print(return_corr)

    Rp = np.dot(return_mean, w)
    Vp = np.sqrt(np.dot(w, np.dot(return_cov, w.T)))
    print(Rp)
    print(Vp)


    x_2000 = np.random.random((len(return_mean.index), 2000))
    w_2000 = x_2000/np.sum(x_2000, axis=0)
    print(w_2000)

