import pandas as pd
import pymongo
import datetime
import dataconnection as dc


def preptradecaldate(db):

    start_dt = datetime.datetime(1990, 1, 1).strftime('%Y%m%d')  # 没有记录的最初日期

    if db['stock_trade_cal'].count_documents({}) != 0:
        try:
            results = db['stock_trade_cal'].find({}).sort('cal_date', pymongo.DESCENDING).limit(1)
            last_date = datetime.datetime.strptime(results[0]['cal_date'], '%Y%m%d')
            start_dt = (last_date + datetime.timedelta(days=1))
            start_dt = start_dt.strftime('%Y%m%d')

        except Exception as exp:
            print(exp)

    return start_dt


def loadtradecalfromtushare(pro, st_dt, ed_dt):

    df = pd.DataFrame()
    try:

        df = pro.trade_cal(exchange='', start_date=st_dt, end_date=ed_dt,
                           fields='exchange,cal_date,is_open,pretrade_date')

    except Exception as exp:
        print(exp)

    return df


def savetradecaltomongo(db, data):
    try:

        result = data.to_dict(orient='records')
        db['stock_trade_cal'].insert_many(result)

    except Exception as exp:
        print(exp)


def runallstock(pro, db):

    print('---开始下载日历数据---')
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))

    # 原始基础数据删除
    st_dtstring = preptradecaldate(db)

    # 从tushare读取基础数据

    ed_dttime = datetime.datetime.now()
    ed_dtstring = (ed_dttime.strftime('%Y%m%d'))

    data = loadtradecalfromtushare(pro, st_dtstring, ed_dtstring)

    # 如果数据不为空，保存数据到mongo
    if not data.empty:
        savetradecaltomongo(db, data)

    t_end = datetime.datetime.now()
    print("程序结束时间：{0}".format(str(t_end)))
    print("程序用时：{0}".format(t_end-t_start))
    print('---日历数据下载结束---')


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
