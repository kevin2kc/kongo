"""
所有股票区分三年，存储在daily_basic的目录里面
"""

import pandas as pd
import pymongo
import datetime
import dataconnection as dc


def prepstockdate(pro, db, stock_pool, ed_date):

    df, df1, df2, results = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    try:
        results = db.stock_daily_basic.find({'ts_code': stock_pool}).sort("trade_date", pymongo.DESCENDING).limit(1)
    except Exception as exp:
        print("daily_basic"+ stock_pool + " : ")
        print(exp)

    if db.stock_daily_basic.count_documents({'ts_code': stock_pool}) == 0:
        ed_dtstr = ed_date.strftime('%Y%m%d')
        try:

            df = pro.daily_basic(ts_code=stock_pool, start_date="20100101", end_date=ed_dtstr)
            df1 = pro.daily_basic(ts_code=stock_pool, start_date="20000101", end_date="20091231")
            df2 = pro.daily_basic(ts_code=stock_pool, start_date="19900101", end_date="19991231")

        except Exception as err:
            print("daily_basic" + stock_pool + " : ")
            print(err)

        if not (df is None):
            df = df["trade_date"]
        if not (df1 is None):
            df1 = df1["trade_date"]
            df = df.append(df1, ignore_index=True, verify_integrity=True)
        if not (df1 is None):
            df2 = df2["trade_date"]
            df = df.append(df2, ignore_index=True, verify_integrity=True)

        df.sort_values(ascending=False, inplace=True)

        if not df.empty:
            start_dt = datetime.datetime.strptime(df.iloc[-1], "%Y%m%d")
        else:
            start_dt = datetime.datetime.now()

    else:
        last_date = datetime.datetime.strptime(results[0]['trade_date'], '%Y%m%d')
        start_dt = (last_date + datetime.timedelta(days=1))
        start_dt.replace(hour=18)

    return start_dt


def loadstockfromtusharebyyear(pro, stock_pool, st_date, ed_date):

    df = pd.DataFrame()
    try:

        df = pro.daily_basic(ts_code=stock_pool, start_date=st_date, end_date=ed_date)

    except Exception as exp:
        print("daily_basic" + stock_pool + " : ")
        print(exp)

    return df


def loadstockfromtushare(pro, stock_pool, start_dttime, end_dttime):

    # 按每年取数进行拼接
    df = pd.DataFrame()
    print("daily_basic 启动下载{0:s}数据".format(stock_pool))

    for year in range(end_dttime.year, start_dttime.year-1, -1):
        # 当年更新的
        if start_dttime.year == datetime.datetime.now().year:

            st_dtstring = start_dttime.strftime('%Y%m%d')
            ed_dtstring = end_dttime.strftime('%Y%m%d')

            print("daily_basic 开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
            df = loadstockfromtusharebyyear(pro, stock_pool, st_dtstring, ed_dtstring)

        else:
            if year == datetime.datetime.now().year:
                st_dtstring = str(year)+"0101"
                ed_dtstring = str(end_dttime.strftime('%Y%m%d'))
                # print("开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
                df = loadstockfromtusharebyyear(pro, stock_pool, st_dtstring, ed_dtstring)
                continue
            elif year == start_dttime.year:
                st_dtstring = start_dttime.strftime('%Y%m%d')
                ed_dtstring = str(year) + "1231"
                # print("开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
                df1 = loadstockfromtusharebyyear(pro, stock_pool, st_dtstring, ed_dtstring)
            else:
                st_dtstring = str(year) + "0101"
                ed_dtstring = str(year) + "1231"
                # print("开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
                df1 = loadstockfromtusharebyyear(pro, stock_pool, st_dtstring, ed_dtstring)

            if not (df1 is None):
                df = pd.concat([df, df1], axis=0)

    return df


def savestocktomongo(db, data, stock_pool):
    try:
        result = data.to_dict('records')
        db.stock_daily_basic.insert_many(result)
    except Exception as exp:
        print("daily_basic" + stock_pool + " : ")
        print(exp)


def runallstock(pro, db):
    # 获得跟股票数据池接口
    data = dc.Connection().getstockbasicallfrommongo()
    # 设定需要获取数据的股票池
    stock_pool = data['ts_code'].tolist()

    print('---daily_basic 开始下载数据---')
    t_start=datetime.datetime.now()
    print("daily_basic 程序开始时间：{0}".format(str(t_start)))

    # 遍历所有股票
    for i in range(len(stock_pool)):

        print("# daily_basic 第{0:d}条数据下载，共{1:d}个 #".format(i + 1, len(stock_pool)))
        # 获取单个股票最后开始时间
        start_dttime = prepstockdate(pro, db, stock_pool[i], datetime.datetime.now())

        if start_dttime > datetime.datetime.now():
            print("daily_basic {0:s}:已经取到最新数据".format(stock_pool[i]))
            continue
        else:

            start_dtstring = start_dttime.strftime("%Y%m%d")
            end_dttime = datetime.datetime.now()
            end_dtstring = (end_dttime.strftime('%Y%m%d'))
            print("daily_basic {0}:下载开始日期:{1}".format(stock_pool[i], start_dtstring))
            print("daily_basic {0}:下载结束日期:{1}".format(stock_pool[i], end_dtstring))

            data = loadstockfromtushare(pro, stock_pool[i], start_dttime, end_dttime)
            savestocktomongo(db, data, stock_pool[i])
            print("daily_basic 结束下载{0}数据".format(stock_pool[i]))
    # ========================================
    t_end=datetime.datetime.now()
    print("daily_basic 程序结束时间：{0}".format(str(t_end)))
    print("daily_basic 程序用时：{0}".format(t_end-t_start))
    print('---daily_basic 全部数据下载结束---')


def main():
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    print("程序开始时间：{0}".format(str(datetime.datetime.now())))

    # 连接mangoDB
    db = dc.Connection().getmongoconnection()
    # 连接tushare
    pro = dc.Connection().gettushareconnection()

    # 运行主程序
    runallstock(pro, db)

    print("程序结束时间：{0}".format(str(datetime.datetime.now())))


if __name__ == '__main__':
    main()
