import dataconnection as dc
import datetime
import pandas as pd
import pymongo


def prepdate(db, col_name):

    start_dt = datetime.datetime(1900, 1, 1)  # datetime.datetime(1990, 1, 1).strftime('%Y%m%d')  # 没有记录的最初日期

    if db[col_name].count_documents({}) != 0:
        try:
            results = db[col_name].find({}).sort('date', pymongo.DESCENDING).limit(1)
            last_date = datetime.datetime.strptime(results[0]['date'], '%Y%m%d')
            start_dt = (last_date + datetime.timedelta(days=1))
            # start_dt = start_dt.strftime('%Y%m%d')
        except Exception as exp:
            print(exp)

    return start_dt


def loaddatafromtusharebyyear(pro, st_dt, ed_dt):

    df = pd.DataFrame()
    try:

        df = pro.shibor(start_date=st_dt, end_date=ed_dt)

    except Exception as exp:
        print(exp)

    return df


def loaddatafromtushare(pro, start_dttime, end_dttime):

    # 按每年取数进行拼接
    df = pd.DataFrame()

    for year in range(end_dttime.year, start_dttime.year-1, -1):
        # 当年更新的
        if start_dttime.year == datetime.datetime.now().year:

            st_dtstring = start_dttime.strftime('%Y%m%d')
            ed_dtstring = end_dttime.strftime('%Y%m%d')

            print("开始时间{0},结束时间{1}".format(st_dtstring, ed_dtstring))
            df = loaddatafromtusharebyyear(pro, st_dtstring, ed_dtstring)

        else:
            if year == datetime.datetime.now().year:
                st_dtstring = str(year)+"0101"
                ed_dtstring = str(end_dttime.strftime('%Y%m%d'))
                df = loaddatafromtusharebyyear(pro, st_dtstring, ed_dtstring)
                continue
            elif year == start_dttime.year:
                st_dtstring = start_dttime.strftime('%Y%m%d')
                ed_dtstring = str(year) + "1231"
                df1 = loaddatafromtusharebyyear(pro, st_dtstring, ed_dtstring)
            else:
                st_dtstring = str(year) + "0101"
                ed_dtstring = str(year) + "1231"
                df1 = loaddatafromtusharebyyear(pro, st_dtstring, ed_dtstring)

            if not df1.empty:
                df = pd.concat([df, df1], axis=0)

    return df


def savedatatomongo(db, data, col_name):
    try:

        result = data.to_dict(orient='records')
        db[col_name].insert_many(result)

    except Exception as exp:
        print(exp)


def runallstock(pro, db, col_name):

    print('---开始下载日历数据---')
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))

    # 原始基础数据删除
    st_dt = prepdate(db, col_name)

    # 从tushare读取基础数据

    ed_dt = datetime.datetime.now()
    # ed_dtstring = (ed_dttime.strftime('%Y%m%d'))

    data = loaddatafromtushare(pro, st_dt, ed_dt)

    # 如果数据不为空，保存数据到mongo
    if not data.empty:
        savedatatomongo(db, data, col_name)

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

    # 运行主程序
    runallstock(pro, db, "data_shibor")
    # df = pro.shibor(start_date="19900101")
    # print(df)

    print("程序结束时间：{0}".format(str(datetime.datetime.now())))


if __name__ == '__main__':
    main()
