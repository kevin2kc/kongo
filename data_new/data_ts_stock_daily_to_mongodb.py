import pandas as pd
import pymongo
import tushare as ts
import datetime
import dataconnection as dc


def prepstockdate(db, pro, stock_pool, ed_date, col_name):

    df, df1, df2, results = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        results = db[col_name].find({'ts_code': stock_pool}).sort("trade_date", pymongo.DESCENDING).limit(1)
    except Exception as exp:
        print("stock_daily" + stock_pool + " : ")
        print(exp)

    if db[col_name].count_documents({'ts_code': stock_pool}) == 0:

        ed_dtstr = ed_date.strftime('%Y%m%d')
        try:
            df = ts.pro_bar(ts_code=stock_pool, api=pro, start_date="20100101", end_date=ed_dtstr)
            df1 = ts.pro_bar(ts_code=stock_pool, api=pro, start_date="20000101", end_date="20091231")
            df2 = ts.pro_bar(ts_code=stock_pool, api=pro, start_date="19900101", end_date="19991231")
        except Exception as err:
            print(col_name + stock_pool + " : ")
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


def loadstockfromtusharebyyear(pro, adj, stock_pool, st_date, ed_date):
    df = None

    try:
        df = ts.pro_bar(ts_code=stock_pool, api=pro, adj=adj, start_date=st_date, end_date=ed_date,
                        factors=["tor", "vr"], adjfactor=True)

    except Exception as exp:
        print("stock_all_" + adj + " " + stock_pool + " : ")
        print(exp)

    return df


def loadstockfromtushare(pro, adj, stock_pool, start_dttime, end_dttime):

    # ???????????????????????????
    df = pd.DataFrame()
    print("stock_all_" + adj + " ????????????{0:s}??????".format(stock_pool))

    for year in range(end_dttime.year, start_dttime.year-1, -1):
        # ???????????????
        if start_dttime.year == datetime.datetime.now().year:

            st_dtstring = start_dttime.strftime('%Y%m%d')
            ed_dtstring = end_dttime.strftime('%Y%m%d')

            print("stock_all_"+adj+" ????????????{0},????????????{1}".format(st_dtstring, ed_dtstring))
            df = loadstockfromtusharebyyear(pro, adj, stock_pool, st_dtstring, ed_dtstring)

        else:
            if year == datetime.datetime.now().year:
                st_dtstring = str(year)+"0101"
                ed_dtstring = str(end_dttime.strftime('%Y%m%d'))
                # print("????????????{0},????????????{1}".format(st_dtstring, ed_dtstring))
                df = loadstockfromtusharebyyear(pro, adj, stock_pool, st_dtstring, ed_dtstring)
                continue
            elif year == start_dttime.year:
                st_dtstring = start_dttime.strftime('%Y%m%d')
                ed_dtstring = str(year) + "1231"
                # print("????????????{0},????????????{1}".format(st_dtstring, ed_dtstring))
                df1 = loadstockfromtusharebyyear(pro, adj, stock_pool, st_dtstring, ed_dtstring)
            else:
                st_dtstring = str(year) + "0101"
                ed_dtstring = str(year) + "1231"
                # print("????????????{0},????????????{1}".format(st_dtstring, ed_dtstring))
                df1 = loadstockfromtusharebyyear(pro, adj, stock_pool, st_dtstring, ed_dtstring)

            if not (df1 is None):
                df = pd.concat([df, df1], axis=0)

    return df


def savestocktomongo(db, data, stock_pool, col_name):

    if data is not None:
        try:
            result = data.to_dict('records')
            db[col_name].insert_many(result)
        except Exception as exp:
            print(col_name + stock_pool + " : ")
            print(exp)


def runallstock(pro, db, adj, col_name):

    # ===============?????????????????????,????????????????????????============================
    # connect database
    t_start = datetime.datetime.now()
    print(col_name + "?????????????????????{0}".format(str(t_start)))

    # ??????????????????????????????
    data = dc.Connection().getstockbasicallfrommongo()
    # ????????????????????????????????????
    stock_pool = data['ts_code'].tolist()

    # ??????????????????
    for i in range(len(stock_pool)):

        print(col_name + "# ???{0:d}?????????????????????{1:d}??? #".format(i + 1, len(stock_pool)))
        # ????????????????????????????????????
        start_dttime = prepstockdate(db, pro, stock_pool[i], datetime.datetime.now(), col_name)

        if start_dttime is None:
            print(col_name + "{0:s}:????????????????????????".format(stock_pool[i]))
            continue
        elif start_dttime > datetime.datetime.now():
            print(col_name + "{0:s}:????????????????????????".format(stock_pool[i]))
            continue
        else:

            start_dtstring = start_dttime.strftime("%Y%m%d")
            end_dttime = datetime.datetime.now()
            end_dtstring = (end_dttime.strftime('%Y%m%d'))
            print(col_name + "{0}:??????????????????:{1}".format(stock_pool[i], start_dtstring))
            print(col_name + "{0}:??????????????????:{1}".format(stock_pool[i], end_dtstring))

            data = loadstockfromtushare(pro, adj, stock_pool[i], start_dttime, end_dttime)
            savestocktomongo(db, data, stock_pool[i], col_name)
            print(col_name + "????????????{0}??????".format(stock_pool[i]))
    # ========================================
    print(col_name + '---????????????????????????---')
    t_end = datetime.datetime.now()
    print(col_name + "?????????????????????{0}".format(str(t_end)))
    print(col_name + "??????????????????{0}".format(str(t_end - t_start)))


def main():
    # ===============?????????????????????,????????????????????????============================
    # connect database
    t_start = datetime.datetime.now()
    print("?????????????????????{0}".format(str(t_start)))

    # ??????mangoDB
    db = dc.Connection().getmongoconnection()
    # ??????tushare
    pro = dc.Connection().gettushareconnection()

    # ???????????????
    runallstock(pro, db, 'qfq', 'stock_all_qfq')

    t_end = datetime.datetime.now()
    print("?????????????????????{0}".format(str(t_end)))
    print("??????????????????{0}".format(str(t_end - t_start)))


if __name__ == '__main__':
    main()
