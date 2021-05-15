import pandas as pd
import pymongo
import tushare as ts
import datetime

#根据给定字段更新表和内容

COLLECTION = "stock_fin_cashflow"
METHOD = "CASHFLOW"


# 获取excel表上关于tushare调用的长参数，做全数据表
def getParameterFromExcel(filename, column_name):
    data = pd.read_excel(filename, engine="openpyxl")
    fields = data[column_name].tolist()
    field_str = ''
    for field in fields:
        field_str = field_str + field + ","
    field_str = field_str[0:len(field_str) - 1]

    return field_str


# 函数返回数据库中单个股票最后需要开始的日期
def prepStockDate(db, stock_pool, ed_date):
    df = None
    try:
        results = db[COLLECTION].find({'ts_code': stock_pool}).sort("end_date", pymongo.DESCENDING).limit(1)
    except Exception as exp:
        print(stock_pool + " : ")
        print(exp)

    if db[COLLECTION].count_documents({'ts_code': stock_pool}) == 0:
       # start_dt = datetime.datetime(1990, 1, 1)  # 没有记录的最初日期
       end_dtstr = ed_date.strftime('%Y%m%d')
       try:
            if METHOD == 'INCOME':
                df = pro.income_vip(ts_code=stock_pool)
            if METHOD == 'BALANCE':
                df = pro.balancesheet_vip(ts_code=stock_pool)
            if METHOD == 'CASHFLOW':
                df = pro.cashflow_vip(ts_code=stock_pool)

       except Exception as err:
           print(stock_pool + " : ")
           print(err)

       if not (df is None) and not (df.empty):
           df.sort_values(by=["end_date"], ascending=False, inplace=True)

           start_dt = datetime.datetime.strptime(df.iloc[-1]['end_date'], "%Y%m%d")

       else:
           print("{0:s}:Tushare没有该股票数据，或者该股票太新".format(stock_pool))
           start_dt = None


    else:
        last_date = datetime.datetime.strptime(results[0]['end_date'], '%Y%m%d')
        start_dt = (last_date + datetime.timedelta(days=1))
        start_dt.replace(hour=18)

    return start_dt


def loadStockFinDataFromTushare(pro, stock_pool, st_date, ed_date):

    df = None

    try:

        if METHOD == 'INCOME':
            df = pro.income_vip(ts_code=stock_pool, start_date=st_date, end_date=ed_date)
        elif METHOD == 'BALANCE':
            df = pro.balancesheet_vip(ts_code=stock_pool, start_date=st_date, end_date=ed_date)
        elif METHOD == 'CASHFLOW':
            df = pro.cashflow_vip(ts_code=stock_pool, start_date=st_date, end_date=ed_date)

    except Exception as exp:
        print(stock_pool + " : ")
        print(exp)

    if (df is None) or (df.empty):
        print("{0:s}:已经取到最新数据".format(stock_pool))

    return df


def saveStockToMongo(db, data, stock_pool):
    try:

        result = data.to_dict(orient='records')
        db[COLLECTION].insert_many(result)

    except Exception as exp:
        print(stock_pool + " : ")
        print(exp)


def runAllStock(pro, db):
    # 获得跟股票数据池接口
    data = pro.stock_basic(exchange='', list_status='L')
    # 设定需要获取数据的股票池
    stock_pool = data['ts_code'].tolist()

    # 获取字段参数
    #field_str = getParameterFromExcel("ts_finance_indicator_all_fields.xlsx", "name")

    # 遍历所有股票
    for i in range(len(stock_pool)):

        print("# 第{0:d}条数据下载，共{1:d}个 #".format(i + 1, len(stock_pool)))
        #获取单个股票最后开始时间

        start_dttime = prepStockDate(db, stock_pool[i], datetime.datetime.now())
        if start_dttime is None:
            continue

        if (start_dttime > datetime.datetime.now()):
            print("{0:s}:已经取到最新数据".format(stock_pool[i]))
            continue
        else:

            start_dtstring = start_dttime.strftime("%Y%m%d")
            end_dttime = datetime.datetime.now()
            end_dtstring = (end_dttime.strftime('%Y%m%d'))
            print("{0}:下载开始日期:{1}".format(stock_pool[i], start_dtstring))
            print("{0}:下载结束日期:{1}".format(stock_pool[i], end_dtstring))

            print("开始下载{0}数据".format(stock_pool[i]))
            # 从tushare读取数据
            data = loadStockFinDataFromTushare(pro, stock_pool[i], start_dtstring, end_dtstring)
            # 保存数据到mongo
            if not (data is None):
                saveStockToMongo(db, data, stock_pool[i])
            else:
                continue
            print("结束下载{0}数据".format(stock_pool[i]))

    # ========================================
    print('---全部数据下载结束---')


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