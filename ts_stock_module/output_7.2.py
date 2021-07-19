import kongo
import numpy as np
import pandas as pd
import matplotlib
from pylab import mpl
import scipy.optimize as sco

mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


def getformuladata(st_date, ed_date, ts_codes, index_codes="", freq="D"):

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


def F(w):

    return_cov = stock_return.cov()*np.sqrt(252)

    Rf = 0.03
    w = np.array(w)
    Rp = np.sum(w * return_mean)
    Vp = np.sqrt(np.dot(w, np.dot(return_cov, w.T)))
    SR = (Rp-Rf)/Vp
    print(SR)
    return np.array([Rp, Vp, SR])

def SRmin_F(w):
    return -F(w)[2]


if __name__ == '__main__':

    df = kongo.Kongo().getstockbasicallfrommongo()
    x = np.random.randint(0, len(df), 10)
    df2 = df.iloc[x]
    stock_list = pd.Series(df2['ts_code']).to_list()

    stock_return, return_mean, return_volatility = getformuladata("20190701", "20210630", stock_list)

    print(return_mean)

    cons = ({'type': 'eq', 'fun': lambda x: np.sum(x)-1})
    bnds = tuple((0, 1) for x in range(len(return_mean)))
    w0 = np.ones_like(return_mean)/len(return_mean.index)

    result = sco.minimize(SRmin_F, w0, method='SLSQP', bounds=bnds, constraints=cons)
    print(result)
    weight = result['x']
    stock_name = stock_list

    for i in range(len(return_mean.index)):
        print(stock_name[i], round(weight[i], 6))





