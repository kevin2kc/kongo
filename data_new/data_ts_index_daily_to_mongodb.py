import pandas as pd
import datetime
import pymongo
import dataconnection as dc


def loadindexbasicfromtushare(pro, market_name):

    df = pd.DataFrame()
    try:
        df = pro.index_basic(market=market_name)
    except Exception as exp:
        print(exp)

    return df


def saveindexbasictomongo(db, data, col_name):
    try:

        result = data.to_dict(orient='records')
        for item in result:
            db[col_name].update_one(item, {'$set': item}, upsert=True)
    except Exception as exp:
        print(exp)


# 函数返回数据库中单个股票最后需要开始的日期
def prepindexdate(pro, db, stock, col_name):

    df, results = pd.DataFrame(), pd.DataFrame()

    try:
        results = db[col_name].find({'ts_code': stock}).sort("trade_date", pymongo.DESCENDING).limit(1)
    except Exception as exp:
        print("finance_indicator" + stock + " : ")
        print(exp)

    if db[col_name].count_documents({'ts_code': stock}) == 0:

        try:
            df = pro.index_daily(ts_code=stock)
        except Exception as err:
            print("index_daily" + stock + " : ")
            print(err)

        if df is not None and not df.empty:
            df.sort_values(by=["trade_date"], ascending=False, inplace=True)
            start_dt = datetime.datetime.strptime(df.iloc[-1][1], "%Y%m%d")

        else:
            print("index daily {0}:Tushare没有该股票数据，或者该股票太新".format(stock))
            start_dt = None

    else:

        last_date = datetime.datetime.strptime(results[0]['end_date'], '%Y%m%d')
        start_dt = (last_date + datetime.timedelta(days=1))
        start_dt.replace(hour=18)

    return start_dt


def loadindexdatafromtushare(pro, index_code, st_date, ed_date):

    df = pd.DataFrame()
    try:
        df = pro.index_daily(ts_code=index_code, start_date=st_date, end_date=ed_date)
    except Exception as exp:
        print(exp)
    if df is None:
        print("index_data {0:s}:已经取到最新数据".format(index_code))
    return df


def saveindexdatatomongo(db, data, index_code, col_name):
    try:

        result = data.to_dict(orient='records')
        db[col_name].insert_many(result)

    except Exception as exp:
        print("index_data" + index_code + " : ")
        print(exp)


def runallstock(pro, db, index_col_name, data_col_name, market_list):

    print('---开始下载日历数据---')
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))

    for market in market_list:
        # 从tushare读取index基础数据
        data1 = loadindexbasicfromtushare(pro, market)
        # 如果数据不为空，保存数据到mongo
        print("正在写入{0}市场的数据...".format(market))
        saveindexbasictomongo(db, data1, index_col_name)
        # 获取所有index列表

    df = dc.Connection().getindexbasicallfrommongo()
    index_pool = df['ts_code'].tolist()

    # 遍历所有股票
    for i in range(len(index_pool)):

        print("# index_daily 第{0:d}条数据下载，共{1:d}个 #".format(i + 1, len(index_pool)))
        # 获取单个股票最后开始时间

        start_dttime = prepindexdate(pro, db, index_pool[i], data_col_name)
        if start_dttime is None:
            print("index_daily {0:s}:已经取到最新数据".format(index_pool[i]))
            continue
        elif start_dttime > datetime.datetime.now():
            print("index_daily {0:s}:已经取到最新数据".format(index_pool[i]))
            continue
        else:
            start_dtstring = start_dttime.strftime("%Y%m%d")
            end_dttime = datetime.datetime.now()
            end_dtstring = (end_dttime.strftime('%Y%m%d'))
            print("index_daily {0}:下载开始日期:{1}".format(index_pool[i], start_dtstring))
            print("index_daily {0}:下载结束日期:{1}".format(index_pool[i], end_dtstring))

            print("index_daily 开始下载{0}数据".format(index_pool[i]))
            # 从tushare读取数据
            data = loadindexdatafromtushare(pro, index_pool[i], start_dtstring, end_dtstring)
            # 保存数据到mongo
            saveindexdatatomongo(db, data, index_pool[i], data_col_name)
            print("index_daily 结束下载{0}数据".format(index_pool[i]))

    t_end = datetime.datetime.now()
    print("程序结束时间：{0}".format(str(t_end)))
    print("程序用时：{0}".format(t_end-t_start))
    print('---日历数据下载结束---')


def main():
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    print("程序开始时间：{0}".format(str(datetime.datetime.now())))

    con = dc.Connection()
    # 连接mangoDB
    db = con.getmongoconnection()
    # 连接tushare
    pro = con.gettushareconnection()

    market = ["SSE", "SZSE"]
    # market = ["SSE", "MSCI", "CSI", "SZSE", "CICC", "SW", "OTH"]

    # 运行主程序
    runallstock(pro, db, "index_basic", "index_data", market)

    print("程序结束时间：{0}".format(str(datetime.datetime.now())))


if __name__ == '__main__':
    main()
