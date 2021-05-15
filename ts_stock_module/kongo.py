"""
This script is to obtain stock information from mongodb database.
"""

import pandas as pd
import pymongo
import numpy as np
import datetime


class Kongo:

    def __init__(self):
        config = {
            'host': 'ubuntu',
            'username': 'admin',
            'password': 'admin',
            'authSource': 'ts_stock',
        }

        client = pymongo.MongoClient(**config)
        self.db = client.ts_stock

    def getDailyBasicFromMongo(self, start_date, end_date, ts_code='', fields=''):

        fields_info = {'ts_code': 1}
        fields = fields.split(',')
        for field in fields:
            fields_info.setdefault(field, 1)

        try:
            if ts_code == '':
                results = self.db['daily_basic'].find({'trade_date': {'$gte': start_date, '$lte': end_date}}, fields_info)\
                    .sort("trade_date", pymongo.DESCENDING)

            else:
                if (isinstance(ts_code, str)):
                    ts_code = [ts_code]
                results = self.db['daily_basic'].find(
                    {'ts_code': {'$in': ts_code}, 'trade_date': {'$gte': start_date, '$lte': end_date}}, fields_info) \
                    .sort("trade_date", pymongo.DESCENDING)
        except Exception as exp:
            print(exp)

        df = pd.DataFrame(results)

        return df


    def getTradeCalFromMongo(self, cal_date):

        try:
            results = self.db['trade_cal'].find({'cal_date' : cal_date})
        except Exception as exp:
            print(exp)
        df = pd.DataFrame(results)

        return df

    def getStockAllFromMongo(self, fields=''):
        fields_info = {'ts_code': 1}
        fields = fields.split(',')
        for field in fields:
            fields_info.setdefault(field, 1)
    
        try:
            results = self.db['stock_basic'].find({}, fields_info)
        except Exception as exp:
            print(exp)
    
        df = pd.DataFrame(results)
    
        return df
    
    def getStockFinCashflowFromMongo(self, start_date, end_date, ts_code='', fields=''):
    
        fields_info = {'ts_code': 1, 'end_date': 1}
        fields = fields.split(',')
        for field in fields:
            fields_info.setdefault(field, 1)
    
        try:
           if ts_code == '':
                results = self.db['stock_fin_cashflow'].find({'end_date': {'$gte': start_date, '$lte': end_date}}, fields_info).sort("trade_date", pymongo.DESCENDING)
    
           else:
                if (isinstance(ts_code, str)):
                    ts_code = [ts_code]
                results = self.db['stock_fin_cashflow'].find({'ts_code': {'$in': ts_code}, 'end_date': {'$gte': start_date, '$lte': end_date}}, fields_info)\
                                                    .sort("trade_date", pymongo.DESCENDING)
        except Exception as exp:
            print(exp)
    
        df = pd.DataFrame(results)
    
        return df
    
    
    def getStockFinIndicatorFromMongo(self,start_date,end_date, ts_code='', fields=''):
    
        fields_info = {'ts_code': 1, 'end_date': 1}
        fields = fields.split(',')
        for field in fields:
            fields_info.setdefault(field, 1)
    
        try:
           if ts_code == '':
                results = self.db['stock_fin_indicator'].find({'end_date': {'$gte': start_date, '$lte': end_date}}, fields_info).sort("trade_date", pymongo.DESCENDING)
    
           else:
                if (isinstance(ts_code, str)):
                    ts_code = [ts_code]
                results = self.db['stock_fin_indicator'].find({'ts_code': {'$in': ts_code}, 'end_date': {'$gte': start_date, '$lte': end_date}}, fields_info)\
                                                    .sort("trade_date", pymongo.DESCENDING)
        except Exception as exp:
            print(exp)
    
        df = pd.DataFrame(results)
    
        return df
    
    
    def getStockDailyFromMongo(self, start_date, end_date, stock_pool='', adj='wfq'):
    
        if (isinstance(stock_pool,str)):
            stock_pool = [stock_pool]
        l=list()
        for stock in stock_pool:
            try:
                if adj == 'wfq':
                    results = self.db[stock].find({'ts_code': stock, 'trade_date': {'$gte': start_date,'$lte': end_date}},
                                                  {"trade_date":1,"ts_code":1, "open":1, "high":1, "low":1, "close":1,
                                                   "pre_close":1, "change":1,"pct_chg":1, "vol":1, "amount":1, "turnover_rate":1,
                                                   "volume_ratio":1}).sort("trade_date", pymongo.DESCENDING)
                elif adj == 'qfq':
                    results = self.db[stock].find({'ts_code': stock, 'trade_date': {'$gte': start_date, '$lte': end_date}},
                                                  {"trade_date": 1, "ts_code": 1, "open_qfq": 1, "high_qfq": 1, "low_qfq": 1, "close_qfq": 1,
                                                   "pre_close_qfq": 1, "change_qfq": 1, "pct_chg_qfq": 1, "vol": 1, "amount": 1, "turnover_rate": 1,
                                                   "volume_ratio": 1}).sort("trade_date", pymongo.DESCENDING)
                elif adj == 'hfq':
                    results = self.db[stock].find({'ts_code': stock, 'trade_date': {'$gte': start_date, '$lte': end_date}},
                                                  {"trade_date": 1, "ts_code": 1, "open_hfq": 1, "high_hfq": 1, "low_hfq": 1,"close_hfq": 1,
                                                   "pre_close_hfq": 1, "change_hfq": 1, "pct_chg_hfq": 1, "vol": 1, "amount": 1,"turnover_rate": 1,
                                                   "volume_ratio": 1}).sort("trade_date", pymongo.DESCENDING)
                else:
                    raise Exception('adj出现错误的问题')
    
            except Exception as exp:
                print(stock + " : ")
                print(exp)
    
            df = pd.DataFrame(results)
            l.append(df)
    
        return df

