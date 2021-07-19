# method 为stock, index, daily_basic, stock_easy 四种
import pandas as pd
import pymongo
import tushare as ts
import datetime
import dataconnection as dc


def prepstockdate(db, pro, stock, col_name):

    df, df1, df2, results = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    flag_result = None

    try:
        flag_result = db.stock_update_flag.find_one({'ts_code': stock}, {'_id': 0, col_name: 1})
    except Exception as exp:
        print(col_name + stock + " : ")
        print(exp)

    # 从update_flag的表取最后时间
    if flag_result != {} and flag_result is not None:
        last_date = datetime.datetime.strptime(flag_result[col_name], '%Y%m%d')
        start_dt = (last_date + datetime.timedelta(days=1))
        start_dt.replace(hour=18)

    # 从数据库拼接最后时间
    else:
        try:
            results = db[col_name].find({'ts_code': stock}).sort("trade_date", pymongo.DESCENDING).limit(1)
        except Exception as exp:
            print(col_name + stock + " : ")
            print(exp)

        if db[col_name].count_documents({'ts_code': stock}) == 0:

            ed_dtstr = datetime.datetime.now().strftime('%Y%m%d')
            try:
                df = ts.pro_bar(ts_code=stock, api=pro, start_date="20100101", end_date=ed_dtstr)
                df1 = ts.pro_bar(ts_code=stock, api=pro, start_date="20000101", end_date="20091231")
                df2 = ts.pro_bar(ts_code=stock, api=pro, start_date="19900101", end_date="19991231")
            except Exception as err:
                print(col_name + stock + " : ")
                print(err)

            if not df.empty:
                df = df["trade_date"]
            if not df1.empty:
                df1 = df1["trade_date"]
                df = df.append(df1, ignore_index=True, verify_integrity=True)
            if not df2.empty:
                df2 = df2["trade_date"]
                df = df.append(df2, ignore_index=True, verify_integrity=True)

            if df is not None and not df.empty:
                df_re = df.copy()
                df_re.sort_values(ascending=False, inplace=True)
                start_dt = datetime.datetime.strptime(df_re.iloc[-1], "%Y%m%d")
            else:
                start_dt = None
        else:

            last_date = datetime.datetime.strptime(results[0]['trade_date'], '%Y%m%d')
            start_dt = (last_date + datetime.timedelta(days=1))
            start_dt.replace(hour=18)

    return start_dt


def loadstockfromtusharebyyear(pro, adj, stock, st_date, ed_date, method, update_flag):

    df = pd.DataFrame()

    try:
        if method == 'stock':
            df = ts.pro_bar(ts_code=stock, api=pro, adj=adj, start_date=st_date, end_date=ed_date,
                            factors=["tor", "vr"], adjfactor=True)

        elif method == 'daily_basic':
            df = pro.daily_basic(ts_code=stock, start_date=st_date, end_date=ed_date)

        elif method == 'index':
            df = pro.index_daily(ts_code=stock, start_date=st_date, end_date=ed_date)

        elif method == 'stock_all_easy' and update_flag is False:
            df = pro.daily(ts_code=stock, start_date=st_date, end_date=ed_date)

        elif method == 'stock_all_easy' and update_flag is True:
            df = pro.daily(trade_date=st_date)

    except Exception as exp:
        print(method + " " + adj + " " + stock + " : ")
        print(exp)

    return pd.DataFrame(df)


def loadstockfromtushare(pro, adj, stock, start_dttime, end_dttime, method, update_flag):

    # 按每年取数进行拼接
    df = pd.DataFrame()
    print(method + adj + " 启动下载{0:s}数据".format(stock))

    for year in range(end_dttime.year, start_dttime.year-1, -1):
        # 当年更新的
        if start_dttime.year == datetime.datetime.now().year:

            st_dtstring = start_dttime.strftime('%Y%m%d')
            ed_dtstring = end_dttime.strftime('%Y%m%d')

            print(method + adj + " 开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
            df = loadstockfromtusharebyyear(pro, adj, stock, st_dtstring, ed_dtstring, method, update_flag)

        else:
            if year == datetime.datetime.now().year:
                st_dtstring = str(year)+"0101"
                ed_dtstring = str(end_dttime.strftime('%Y%m%d'))
                # print("开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
                df = loadstockfromtusharebyyear(pro, adj, stock, st_dtstring, ed_dtstring, method, update_flag)
                continue
            elif year == start_dttime.year:
                st_dtstring = start_dttime.strftime('%Y%m%d')
                ed_dtstring = str(year) + "1231"
                # print("开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
                df1 = loadstockfromtusharebyyear(pro, adj, stock, st_dtstring, ed_dtstring, method, update_flag)
            else:
                st_dtstring = str(year) + "0101"
                ed_dtstring = str(year) + "1231"
                # print("开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
                df1 = loadstockfromtusharebyyear(pro, adj, stock, st_dtstring, ed_dtstring, method, update_flag)

            if not df1.empty:
                df = pd.concat([df, df1], axis=0)

    return df


def updateflagtomongo(db, stock, col_name, date_flag, method, update_flag):

    try:
        if (method == "stock_all_easy") and (update_flag is True):

            db['stock_update_flag'].update_many({}, {'$set': {col_name: date_flag}})

        else:
            db['stock_update_flag'].update_one({"ts_code": stock},
                                               {'$set': {"ts_code": stock, col_name: date_flag}}, upsert=True)

    except Exception as exp:
        print(exp)


def savestocktomongo(db, data, stock, end_date, col_name, method, update_flag):

    if not data.empty:
        try:
            result = data.to_dict('records')
            db[col_name].insert_many(result)
        except Exception as exp:
            print(col_name + stock + " : ")
            print(exp)
        else:
            try:
                updateflagtomongo(db, stock, col_name, end_date, method, update_flag)
                print("更新日期为{0}的标志...".format(end_date))
            except Exception as exp:
                print("update_flag " + col_name + stock + " : ")
                print(exp)


def processallstock(pro, db, stock, adj, col_name, method, update_flag):

    # 获取单个股票最后开始时间
    start_dttime = prepstockdate(db, pro, stock, col_name)
    # start_dttime = datetime.datetime.strptime("20210701", "%Y%m%d")

    if start_dttime is None:
        print(col_name + "{0:s}:已经取到最新数据".format(stock))
        return
    elif start_dttime > datetime.datetime.now():
        print(col_name + "{0:s}:已经取到最新数据".format(stock))
        return
    else:

        start_dtstring = start_dttime.strftime("%Y%m%d")
        end_dttime = datetime.datetime.now()
        end_dtstring = (end_dttime.strftime('%Y%m%d'))
        print(col_name + "{0}:下载开始日期:{1}".format(stock, start_dtstring))
        print(col_name + "{0}:下载结束日期:{1}".format(stock, end_dtstring))

        data = loadstockfromtushare(pro, adj, stock, start_dttime, end_dttime, method, update_flag)

        if not data.empty:
            data_2 = data.sort_values(by="trade_date", ascending=False).copy()
            data_2.reset_index(inplace=True)
            end_dtstring = data_2.loc[0, "trade_date"]
            print("数据不空：数据表最后时间是：{0}".format(end_dtstring))
        else:
            end_dtstring = (end_dttime + datetime.timedelta(days=-1)).strftime("%Y%m%d")
            print("数据为空：数据表最后时间是：{0}".format(end_dtstring))

        savestocktomongo(db, data, stock, end_dtstring, col_name, method, update_flag)
        print(col_name + "结束下载{0}数据".format(stock))


def runallstock(pro, db, adj, col_name, method, update_flag=True):

    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    t_start = datetime.datetime.now()
    print(col_name + "程序开始时间：{0}".format(str(t_start)))

    # 获得跟股票数据池接口

    if method == 'index':
        data = dc.Connection().getindexbasicallfrommongo()
    else:
        data = dc.Connection().getstockbasicallfrommongo()

    # 设定需要获取数据的股票池
    stock_pool = data['ts_code'].tolist()

    # 更新stock_all_easy的方式
    if (method == "stock_all_easy") and (update_flag is True):

        st_date = prepstockdate(db, pro, "000001.SZ", col_name)
        proc_date = st_date

        while proc_date.strftime('%Y%m%d') != datetime.datetime.now().strftime('%Y%m%d'):

            str_proc_date = proc_date.strftime('%Y%m%d')

            print("输入日期为{0}的数据...".format(str_proc_date))
            data = loadstockfromtusharebyyear(pro, 'wfq', '000001.SZ',
                                              str_proc_date, str_proc_date, method, update_flag)

            savestocktomongo(db, data, "000001.SZ", str_proc_date, col_name, method, update_flag)

            proc_date = proc_date + datetime.timedelta(days=1)

    # 其他方式，其他方式都需要遍历所有股票
    else:
        # 遍历所有股票
        for i, stock in enumerate(stock_pool):

            print(col_name + "# 第{0:d}条数据下载，共{1:d}个 #".format(i + 1, len(stock_pool)))

            processallstock(pro, db, stock, adj, col_name, method, update_flag)

        # ========================================
        print(col_name + '---全部数据下载结束---')
        t_end = datetime.datetime.now()
        print(col_name + "程序结束时间：{0}".format(str(t_end)))
        print(col_name + "共花费时间：{0}".format(str(t_end - t_start)))


def main():
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))

    con = dc.Connection()
    # 连接mangoDB
    db = con.getmongoconnection()
    # 连接tushare
    pro = con.gettushareconnection()

    # 运行主程序
    # runallstock(pro, db, 'wfq', 'stock_all_easy', 'stock_all_easy', False)
    # runallstock(pro, db, 'wfq', 'stock_all_wfq', 'stock', False)
    # runallstock(pro, db, 'wfq', 'index_data', 'index', False)
    runallstock(pro, db, 'wfq', 'index_data', 'index', False)


    t_end = datetime.datetime.now()
    print("程序结束时间：{0}".format(str(t_end)))
    print("共花费时间：{0}".format(str(t_end - t_start)))


if __name__ == '__main__':
    main()
