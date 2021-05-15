
import sys
import pymongo
import pprint
import pandas as pd
import numpy as np
import kongo as kg

from openpyxl import load_workbook
from datetime import *
from dateutil.parser import parse
import tushare as ts



# 判断是否为交易日，不为交易日则返回前一个交易日
def is_tradeday(date):
    db = kg.Kongo()
    _df_calendar = db.getTradeCalFromMongo(cal_date=date)
    df_cal = _df_calendar
    if df_cal.loc[0, 'is_open']:
        return date
    else:
        return df_cal.loc[0, 'pretrade_date']



# 获取输入年开始的前三年字符串，返回字符串数组
def get_recent_3years_pe(year):
    re_date = []
    year = is_tradeday(year)
    re_date.append(year)

    date = parse(year)
    last_year = int(date.year) - 1
    str_date = str(last_year) + date.strftime('%m%d')
    str_date = is_tradeday(str_date)
    re_date.append(str_date)

    date = parse(year)
    last_year = int(date.year) - 2
    str_date = str(last_year) + date.strftime('%m%d')
    str_date = is_tradeday(str_date)
    re_date.append(str_date)

    return re_date


# 获取输入年开始的前三年交易日字符串,检查是否是交易日，返回字符串数组
def get_recent_3years_roic(year):
    re_date = []
    re_date.append(year)

    date = parse(year)
    last_year = int(date.year) - 1
    str_date = str(last_year) + date.strftime('%m%d')
    re_date.append(str_date)

    date = parse(year)
    last_year = int(date.year) - 2
    str_date = str(last_year) + date.strftime('%m%d')
    re_date.append(str_date)

    return re_date


# 获取当年开始前三年的PE_TTM数据

def get_pe_3years(period):

    years = get_recent_3years_pe(period)

    db = kg.Kongo()

    _df_list = db.getStockAllFromMongo(fields='ts_code,symbol,name')
    df_pe_ttm = _df_list
    
    _df_pe_ttm_year1 = db.getDailyBasicFromMongo(start_date=years[0], end_date=years[0], fields='pe_ttm')
    _df_pe_ttm_year2 = db.getDailyBasicFromMongo(start_date=years[1], end_date=years[1], fields='pe_ttm')
    _df_pe_ttm_year3 = db.getDailyBasicFromMongo(start_date=years[2], end_date=years[2], fields='pe_ttm')

    df_pe_ttm = pd.merge(df_pe_ttm['ts_code'], _df_pe_ttm_year1[['ts_code', 'pe_ttm']].drop_duplicates(), left_on='ts_code', right_on='ts_code', how='left')
    df_pe_ttm.rename(columns={'pe_ttm': 'pe_ttm_year1'}, inplace=True)

    df_pe_ttm = pd.merge(df_pe_ttm, _df_pe_ttm_year2[['ts_code', 'pe_ttm']].drop_duplicates(), left_on='ts_code', right_on='ts_code', how='left')
    df_pe_ttm.rename(columns={'pe_ttm': 'pe_ttm_year2'}, inplace=True)

    df_pe_ttm = pd.merge(df_pe_ttm, _df_pe_ttm_year3[['ts_code', 'pe_ttm']].drop_duplicates(), left_on='ts_code', right_on='ts_code', how='left')
    df_pe_ttm.rename(columns={'pe_ttm': 'pe_ttm_year3'}, inplace=True)

    df_pe_ttm.fillna(value=0, inplace=True)

    return df_pe_ttm


# 获取当年开始前三年的ROIC数据

def get_roic_3years(period):

    years = get_recent_3years_roic(period)

    db = kg.Kongo()

    _df_list = db.getStockAllFromMongo(fields='ts_code,symbol,name')
    df_roic = _df_list

    _df_roic_year1 = db.getStockFinIndicatorFromMongo(start_date=years[0], end_date=years[0], fields="roic")
    _df_roic_year2 = db.getStockFinIndicatorFromMongo(start_date=years[1], end_date=years[1], fields="roic")
    _df_roic_year3 = db.getStockFinIndicatorFromMongo(start_date=years[2], end_date=years[2], fields="roic")




    df_roic = pd.merge(df_roic[['ts_code','symbol','name']], _df_roic_year1[['ts_code', 'roic']].drop_duplicates(subset=['ts_code']), left_on='ts_code', right_on='ts_code', how='left')
    df_roic.rename(columns={'roic': 'roic_year1'}, inplace=True)

    df_roic = pd.merge(df_roic, _df_roic_year2[['ts_code', 'roic']].drop_duplicates(subset=['ts_code']), left_on='ts_code', right_on='ts_code', how='left')
    df_roic.rename(columns={'roic': 'roic_year2'}, inplace=True)

    df_roic = pd.merge(df_roic, _df_roic_year3[['ts_code', 'roic']].drop_duplicates(subset=['ts_code']), left_on='ts_code', right_on='ts_code', how='left')
    df_roic.rename(columns={'roic': 'roic_year3'}, inplace=True)

    df_roic.fillna({'roic': np.nan}, inplace=True)

    return df_roic


def merge_data(df_roic, df_pe_ttm):

    data = pd.merge(df_roic, df_pe_ttm, left_on="ts_code", right_on="ts_code", how="outer")
    data.reset_index(drop=True, inplace=True)
    return data


# 按当年PE和ROIC计算股票列表, 返回前n个排名，默认前40个
def stock_list(data, top=50, method=0):
    data = data[(data['roic_year1'] > 0) & (data['pe_ttm_year1'] > 0) &
                (data['roic_year2'] > 0) & (data['pe_ttm_year2'] > 0) &
                (data['roic_year3'] > 0) & (data['pe_ttm_year3'] > 0)]

    data = data.copy()

    # 取一年的排名
    if method == 0:
        data.loc[:, 'rank_pe'] = data.loc[:, 'pe_ttm_year1'].rank(method='average')
        data.loc[:, 'rank_roic'] = data.loc[:, 'roic_year1'].rank(ascending=False, method='average')
    # 取非权重三年的排名
    elif method == 1:

        data.loc[:, 'rank_pe'] = data.loc[:, 'pe_ttm_year1'].rank(method='average') + \
                             data.loc[:, 'pe_ttm_year2'].rank(method='average') + \
                             data.loc[:, 'pe_ttm_year3'].rank(method='average')

        data.loc[:, 'rank_roic'] = data.loc[:, 'roic_year1'].rank(ascending=False, method='average') + \
                               data.loc[:, 'roic_year2'].rank(ascending=False, method='average') + \
                               data.loc[:, 'roic_year3'].rank(ascending=False, method='average')
    # 取权重三年的排名
    elif method == 2:
        data.loc[:, 'rank_pe'] = data.loc[:, 'pe_ttm_year1'].rank(method='average') *0.6 + \
                             data.loc[:, 'pe_ttm_year2'].rank(method='average') *0.3 + \
                             data.loc[:, 'pe_ttm_year3'].rank(method='average') *0.1

        data.loc[:, 'rank_roic'] = data.loc[:, 'roic_year1'].rank(ascending=False, method='average') *0.6 + \
                               data.loc[:, 'roic_year2'].rank(ascending=False, method='average') *0.3 + \
                               data.loc[:, 'roic_year3'].rank(ascending=False, method='average') *0.1



    data.loc[:, 'rank'] = data.loc[:, 'rank_pe'] + data.loc[:, 'rank_roic']
    data.sort_values(by='rank', inplace=True)
    data.reset_index(drop=True, inplace=True)

    final_choice = data.iloc[:top, :]

    return final_choice


def report():

    # 生成数据库链接

    #获取前倒退前三年PE TTM数据
    df_pe = get_pe_3years("20200930")
    #获取当前倒退前三年ROIC数据
    df_roic = get_roic_3years("20200930")
    #对三年数据进行整理合并
    df_data = merge_data(df_roic, df_pe)

    df_data.to_excel("data.xlsx", engine="openpyxl")

    #按非权重的方式进行排序，获取前n项的排名
    final_choice_lastyear = stock_list(df_data, top=50, method=0)
    final_choice_threeyear = stock_list(df_data, top=50, method=1)
    final_choice_weight = stock_list(df_data, top=50, method=2)

    match_list1 = pd.merge(final_choice_lastyear,final_choice_threeyear,how="inner",left_on="ts_code",right_on="ts_code",)
    match_list2 = pd.merge(match_list1,final_choice_weight,how="inner",left_on="ts_code",right_on="ts_code")
    final_list = match_list2[["ts_code", "symbol", "name"]]
    
    with pd.ExcelWriter('分析报告.xlsx') as writer:
        final_choice_lastyear.to_excel(writer, sheet_name="最近一年分析结果")
        final_choice_threeyear.to_excel(writer, sheet_name="三年非权重结果")
        final_choice_weight.to_excel(writer, sheet_name="三年不同权重结果")
        final_list.to_excel(writer, sheet_name="三种结果交集")

if __name__ == '__main__':

    # 连接mangoDB
    report()



