import pandas as pd
import pymongo
import dataconnection as dc
import datetime


# 函数返回数据库中单个股票最后需要开始的日期
def prepstockdate(pro, db, stock_pool, method, col_name):
    df, results = pd.DataFrame(), pd.DataFrame()
    try:
        results = db[col_name].find({'ts_code': stock_pool}).sort("end_date", pymongo.DESCENDING).limit(1)
    except Exception as exp:
        print("finance_report" + stock_pool + " : ")
        print(exp)

    if db[col_name].count_documents({'ts_code': stock_pool}) == 0:

        try:
            if method == 'income':
                df = pro.income_vip(ts_code=stock_pool)
            if method == 'balancesheet':
                df = pro.balancesheet_vip(ts_code=stock_pool)
            if method == 'cashflow':
                df = pro.cashflow_vip(ts_code=stock_pool)

        except Exception as err:
            print("finance_report" + stock_pool + " : ")
            print(err)

        if not (df is None) and not df.empty:
            df.sort_values(by=["end_date"], ascending=False, inplace=True)

            start_dt = datetime.datetime.strptime(df.iloc[-1]['end_date'], "%Y%m%d")

        else:
            print("finance_report {0:s}:Tushare没有该股票数据，或者该股票太新".format(stock_pool))
            start_dt = None

    else:
        last_date = datetime.datetime.strptime(results[0]['end_date'], '%Y%m%d')
        start_dt = (last_date + datetime.timedelta(days=1))
        start_dt.replace(hour=18)

    return start_dt


def loadstockfindatafromtushare(pro, stock_pool, st_date, ed_date, method):

    df = None

    try:

        if method == 'income':
            df = pro.income_vip(ts_code=stock_pool, start_date=st_date, end_date=ed_date)
        elif method == 'balancesheet':
            df = pro.balancesheet_vip(ts_code=stock_pool, start_date=st_date, end_date=ed_date)
        elif method == 'cashflow':
            df = pro.cashflow_vip(ts_code=stock_pool, start_date=st_date, end_date=ed_date)

    except Exception as exp:
        print("finance_report" + stock_pool + " : ")
        print(exp)

    if (df is None) or df.empty:
        print("finance_report {0:s}:已经取到最新数据".format(stock_pool))

    return df


def savestocktomongo(db, data, stock_pool, col_name):
    try:

        result = data.to_dict(orient='records')
        db[col_name].insert_many(result)

    except Exception as exp:
        print("finance_report" + stock_pool + " : ")
        print(exp)


def runallstock(pro, db, method, col_name):
    # 获得跟股票数据池接口
    data = dc.Connection().getstockbasicallfrommongo()
    # 设定需要获取数据的股票池
    stock_pool = data['ts_code'].tolist()

    print('---finance_report 开始下载数据---')
    t_start=datetime.datetime.now()
    print("finance_report 程序开始时间：{0}".format(str(t_start)))

    # 遍历所有股票
    for i in range(len(stock_pool)):

        print("# finance_report 第{0:d}条数据下载，共{1:d}个 #".format(i + 1, len(stock_pool)))
        # 获取单个股票最后开始时间

        start_dttime = prepstockdate(pro, db, stock_pool[i], method, col_name)
        if start_dttime is None:
            continue

        if start_dttime > datetime.datetime.now():
            print("finance_report {0:s}:已经取到最新数据".format(stock_pool[i]))
            continue
        else:

            print("finance_report {0}:下载开始日期:{1}".format(stock_pool[i], start_dttime.strftime("%Y%m%d")))
            print("finance_report {0}:下载结束日期:{1}".format(stock_pool[i], datetime.datetime.now().strftime('%Y%m%d')))
            print("finance_report 开始下载{0}数据".format(stock_pool[i]))
            # 从tushare读取数据
            data = loadstockfindatafromtushare(pro, stock_pool[i], start_dttime.strftime("%Y%m%d"),
                                               datetime.datetime.now().strftime('%Y%m%d'), method)
            # 保存数据到mongo
            if not (data is None):
                savestocktomongo(db, data, stock_pool[i], col_name)
            else:
                continue
            print("finance_report 结束下载{0}数据".format(stock_pool[i]))

    # ========================================
    t_end=datetime.datetime.now()
    print("finance_report 程序结束时间：{0}".format(str(t_end)))
    print("finance_report 程序用时：{0}".format(t_end-t_start))
    print('---finance_report 全部数据下载结束---')


def main():
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))

    # 连接mangoDB
    db = dc.Connection().getmongoconnection()
    # 连接tushare
    pro = dc.Connection().gettushareconnection()

    # 运行主程序
    runallstock(pro, db, 'cashflow', 'cashflow')

    t_end = datetime.datetime.now()
    print("程序结束时间：{0}".format(str(t_end)))
    print("共花费时间：{0}".format(str(t_end - t_start)))


if __name__ == '__main__':
    main()
