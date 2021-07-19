"""
This script is to obtain stock information from mongodb database.
"""

import pandas as pd
import pymongo


class Kongo(object):

    def __init__(self):
        config = {
            'host': 'DESKTOPBLUE',
            'username': 'tushare',
            'password': 'tushare',
            'authSource': 'ts_stock',
        }

        client = pymongo.MongoClient(**config)
        self.db = client.ts_stock

    # 通过代码获取股票名称
    def getindextscodebyname(self, name):

        if isinstance(name, str):
            name = name.split(',')

        results = self.db['index_basic'].find(
            {'name': {'$in': name}}, {'_id': 0, 'name': 1, 'ts_code': 1})
        return pd.DataFrame(results)

    # 获取股票的基本信息
    def getstocktinfo(self, para, method="name"):

        if isinstance(para, str):
            para = para.split(',')

        results = self.db['stock_basic'].find(
            {method: {'$in': para}}, {'_id': 0, 'name': 1, 'ts_code': 1})

        return pd.DataFrame(results)

    # 获取股票每日基本信息
    def getstockdailybasicfrommongo(self, start_date, end_date, ts_code='', fields=''):

        results = pd.DataFrame()

        fields_info = {'_id': 0}
        if fields != '':
            fields = fields.split(',')
            for field in fields:
                fields_info.setdefault(field, 1)

        try:
            if ts_code == '':
                results = self.db['stock_daily_basic'].find({'trade_date': {'$gte': start_date, '$lte': end_date}},
                     fields_info).sort("trade_date", pymongo.ASCENDING).allow_disk_use(True)

            else:
                if isinstance(ts_code, str):
                    ts_code = ts_code.split(',')
                results = self.db['stock_daily_basic'].find(
                    {'ts_code': {'$in': ts_code}, 'trade_date': {'$gte': start_date, '$lte': end_date}}, fields_info) \
                    .sort("trade_date", pymongo.ASCENDING).allow_disk_use(True)
        except Exception as exp:
            print(exp)

        df = pd.DataFrame(results)

        return df

    # 获取交易日历
    def gettradecalfrommongo(self, start_date, end_date):

        results = pd.DataFrame()
        try:
            results = self.db['stock_trade_cal'].find({'cal_date': {'$gte': start_date, '$lte': end_date}}, {'_id': 0})
        except Exception as exp:
            print(exp)
        df = pd.DataFrame(results)

        return df

    # 获取无风险利率
    def getshiborfrommongo(self, st_date, ed_date, sb_type='1y'):

        results = pd.DataFrame()
        try:
            results = self.db['data_shibor'].find({'date': {'$gte': st_date, '$lte': ed_date}}, {"_id": 0, sb_type: 1})
        except Exception as exp:
            print(exp)
        df = pd.DataFrame(results)

        return df

    # 获取所有股票的基本信息
    def getstockbasicallfrommongo(self):

        results = pd.DataFrame()
        try:
            results = self.db['stock_basic'].find({'list_status': {'$eq': 'L'}}, {'_id': 0})
        except Exception as exp:
            print(exp)
        df = pd.DataFrame(results)

        return df

    # 获取三大报表数据，默认为资产负债表
    def getstockfinreportfrommongo(self, start_date, end_date, stock_pool='', fields='', method="balancesheet"):

        results = pd.DataFrame()

        if isinstance(stock_pool, str):
            stock_pool = stock_pool.split(',')

        fields_info = {'_id': 0, 'ts_code': 1, 'end_date': 1}
        if fields != '':
            fields = fields.split(',')
            for field in fields:
                fields_info.setdefault(field, 1)

        try:
            if stock_pool == ['']:
                results = self.db['stock_report_'+method].\
                    find({'end_date': {'$gte': start_date, '$lte': end_date}},
                         fields_info).sort("trade_date", pymongo.ASCENDING)
    
            else:
                results = self.db['stock_report_'+method].\
                    find({'ts_code': {'$in': stock_pool}, 'end_date': {'$gte': start_date, '$lte': end_date}},
                         fields_info).sort("trade_date", pymongo.ASCENDING)
        except Exception as exp:
            print(exp)
    
        df = pd.DataFrame(results)
    
        return df

    # 获取财务指标信息
    def getstockfinindicatorfrommongo(self, start_date, end_date, ts_code='', fields=''):

        results = pd.DataFrame()

        fields_info = {'_id': 0, 'ts_code': 1, 'end_date': 1}
        if fields != '':
            fields = fields.split(',')
            for field in fields:
                fields_info.setdefault(field, 1)
    
        try:
            if ts_code == '':
                results = self.db['stock_fin_indicator'].find({'end_date': {'$gte': start_date, '$lte': end_date}},
                                                              fields_info).sort("end_date", pymongo.ASCENDING).\
                                                              allow_disk_use(True)
    
            elif isinstance(ts_code, str):
                ts_code = ts_code.split(',')
                results = self.db['stock_fin_indicator'].find(
                    {'ts_code': {'$in': ts_code}, 'end_date': {'$gte': start_date, '$lte': end_date}}, fields_info). \
                    sort("end_date", pymongo.ASCENDING).allow_disk_use(True)
        except Exception as exp:
            print(exp)
    
        df = pd.DataFrame(results)
    
        return df

    # 获取一个或一组股票或者指数的期间交易基础数据，返回dataframe
    def getstockdailyfrommongo(self, start_date, end_date, stock_pool='', method='stock', adj='wfq'):

        results, df = pd.DataFrame(), pd.DataFrame()

        if isinstance(stock_pool, str):
            stock_pool = stock_pool.split(',')

        if method == 'stock':
            try:
                if adj == 'wfq':
                    results = self.db["stock_all_" + adj].find(
                        {'ts_code': {'$in': stock_pool}, 'trade_date': {'$gte': start_date, '$lte': end_date}},
                        {'_id': 0}).sort("trade_date", pymongo.ASCENDING).allow_disk_use(True)
                elif adj == 'qfq':
                    results = self.db["stock_all_" + adj].find(
                        {'ts_code': {'$in': stock_pool}, 'trade_date': {'$gte': start_date, '$lte': end_date}},
                        {'_id': 0}).sort("trade_date", pymongo.ASCENDING).allow_disk_use(True)
                elif adj == 'hfq':
                    results = self.db["stock_all_" + adj].find(
                        {'ts_code': {'$in': stock_pool}, 'trade_date': {'$gte': start_date, '$lte': end_date}},
                        {'_id': 0}).sort("trade_date", pymongo.ASCENDING).allow_disk_use(True)
                else:
                    raise Exception('adj出现错误的问题')
                df = pd.DataFrame(results)

            except Exception as exp:
                print(exp)

        elif method == 'index':
            try:
                results = self.db["index_data"].find(
                    {'ts_code': {'$in': stock_pool}, 'trade_date': {'$gte': start_date, '$lte': end_date}},
                    {'_id': 0}).sort("trade_date", pymongo.ASCENDING).allow_disk_use(True)
                df = pd.DataFrame(results)

            except Exception as exp:
                print(exp)

        return df

