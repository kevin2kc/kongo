import pymongo
import tushare as ts
# connect database

# 连接mangoDB

def mongodbconnection():
    config = {
        'host': 'localhost',
        'username': 'tushare',
        'password': 'tushare',
        'authSource': 'ts_stock',
    }

    client = pymongo.MongoClient(**config)
    db = client.ts_stock
    return db

# 连接tushare


def tushareconnection():
    token = 'dfb6e9f4f9a3db86c59a3a0f680a9bdc46ed1b5adbf1e354c7faa761'
    pro = ts.pro_api(token)
    return pro
