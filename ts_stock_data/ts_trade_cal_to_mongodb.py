import pandas as pd
import pymongo
import tushare as ts
import datetime

def prepTradeCalDate(db):

    if db['trade_cal'].count_documents({}) == 0:
       start_dt = datetime.datetime(1990, 1, 1).strftime('%Y%m%d')  # 没有记录的最初日期
       return start_dt

    else:
       try:
           results = db['trade_cal'].find({}).sort('cal_date', pymongo.DESCENDING).limit(1)
           last_date = datetime.datetime.strptime(results[0]['cal_date'], '%Y%m%d')
           start_dt = (last_date + datetime.timedelta(days=1))
           start_dt = start_dt.strftime('%Y%m%d')

       except Exception as exp:
           print(exp)

       '''
       if not (df is None) and not (df.empty):
           df.sort_values(by=["end_date"], ascending=False, inplace=True)

           start_dt = datetime.datetime.strptime(df.iloc[-1][2], "%Y%m%d")

       else:
       
           print("{0:s}:Tushare没有该股票数据，或者该股票太新".format(stock_pool))
           start_dt = None
       '''


    return start_dt


def loadTradeCalFromTushare(pro, st_dt, ed_dt):

    df = pd.DataFrame()
    try:

        df = pro.trade_cal(exchange='', start_date=st_dt, end_date=ed_dt, fields='exchange,cal_date,is_open,pretrade_date')

    except Exception as exp:
        print(exp)

    return df


def saveTradeCalToMongo(db, data):
    try:

        result = data.to_dict(orient='records')
        db['trade_cal'].insert_many(result)

    except Exception as exp:
        print(exp)


def runAllStock(pro, db):

    # 原始基础数据删除
    st_dtstring = prepTradeCalDate(db)

    # 从tushare读取基础数据

    ed_dttime = datetime.datetime.now()
    ed_dtstring = (ed_dttime.strftime('%Y%m%d'))

    data = loadTradeCalFromTushare(pro, st_dtstring, ed_dtstring)

    # 如果数据不为空，保存数据到mongo
    if not data.empty:
        saveTradeCalToMongo(db, data)

    # ========================================
    print('---全部基础数据下载结束---')


if __name__ == '__main__':
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))

    # 连接mangoDB
    config = {
        'host': 'ubuntu',
        'username': 'admin',
        'password': 'admin',
        'authSource': 'ts_stock',
    }

    client = pymongo.MongoClient(**config)
    db = client.ts_stock

    # 连接tushare
    token = 'dfb6e9f4f9a3db86c59a3a0f680a9bdc46ed1b5adbf1e354c7faa761'
    pro = ts.pro_api(token)

    # 运行主程序
    runAllStock(pro, db)

    t_end = datetime.datetime.now()
    print("程序结束时间：{0}".format(str(t_end)))
    print("共花费时间：{0}".format(str(t_end - t_start)))