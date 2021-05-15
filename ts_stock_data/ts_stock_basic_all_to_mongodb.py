import pandas as pd
import pymongo
import tushare as ts
import datetime

def dropStockBasicFromTushare(db):

    try:
        db.drop_collection("stock_basic")
    except Exception as exp:
        print(exp)


def loadStockBasicFromTushare(pro):

    df = pd.DataFrame()
    try:

        df = pro.stock_basic(exchange='', list_status='L')

    except Exception as exp:
        print(exp)

    return df


def saveStockBasicToMongo(db, data):
    try:

        result = data.to_dict(orient='records')
        db['stock_basic'].insert_many(result)

    except Exception as exp:
        print(exp)


def runAllStock(pro, db):

    # 原始基础数据删除
    dropStockBasicFromTushare(db)
    # 从tushare读取基础数据
    data = loadStockBasicFromTushare(pro)
    # 保存数据到mongo
    saveStockBasicToMongo(db, data)


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