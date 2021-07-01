import pymongo
import tushare as ts
import pandas as pd


# 连接tushare

class Connection:

    def __init__(self):

        token = 'dfb6e9f4f9a3db86c59a3a0f680a9bdc46ed1b5adbf1e354c7faa761'
        self.pro = ts.pro_api(token)

        config = {
            'host': 'DESKTOPBLUE',
            'username': 'tushare',
            'password': 'tushare',
            'authSource': 'ts_stock',
        }

        client = pymongo.MongoClient(**config)
        self.db = client.ts_stock

    def gettushareconnection(self):

        return self.pro

    def getmongoconnection(self):

        return self.db

    def getstockbasicallfrommongo(self):

        df, results = pd.DataFrame(), pd.DataFrame()
        try:
            results = self.db['stock_basic'].find({'list_status': {'$eq': 'L'}}, {'ts_code': 1})
        except Exception as exp:
            print(exp)
        df = pd.DataFrame(results)

        return df
