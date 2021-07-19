import threading
import sys
import datetime
import data_logger

import dataconnection
import data_ts_stock_daily_to_mongodb
import data_ts_finance_indicator_to_mongodb
import data_ts_finance_report_to_mongodb
import basicdata_ts_trade_cal_to_mongodb
import data_ts_daily_basic_to_mongodb
import basicdata_ts_stock_basic_to_mongodb
import basicdata_ts_index_basic_to_mongodb
import prefinal_data_ts_stock_daily_to_mongodb

if __name__ == '__main__':

    tsname = datetime.datetime.now().date().strftime("%Y%m%d")
    sys.stdout = data_logger.Logger("log/" + tsname + ".log", sys.stdout)
    sys.stderr = data_logger.Logger("log/" + tsname + "err.log", sys.stderr)

    print('---kongo开始下载数据---')
    t_start = datetime.datetime.now()
    print("kongo程序开始时间：{0}".format(str(t_start)))

    # 连接mangoDB
    db = dataconnection.Connection().getmongoconnection()

    # 连接tushare
    pro = dataconnection.Connection().gettushareconnection()

    # 基础股票信息
    basicdata_ts_stock_basic_to_mongodb.runallstock(pro, db, "stock_basic")
    # 基础指数信息
    basicdata_ts_index_basic_to_mongodb.runallstock(pro, db, "index_basic", ["SSE", "SZSE"])
    # 基础交易日信息
    basicdata_ts_trade_cal_to_mongodb.runallstock(pro, db)

    # 指数日数据
    index_data = threading.Thread(target=prefinal_data_ts_stock_daily_to_mongodb.runallstock,
                                  args=(pro, db, 'wfq', 'index_data', 'index', False))
    index_data.start()

    stock_wfq = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'wfq', 'stock_all_wfq'))
    stock_hfq = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'hfq', 'stock_all_hfq'))
    stock_qfq = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'qfq', 'stock_all_qfq'))


    stock_wfq.start()
    stock_qfq.start()
    stock_hfq.start()

    daily_basic = threading.Thread(target=data_ts_daily_basic_to_mongodb.runallstock, args=(pro, db))
    daily_basic.start()

    """
    fin_indicator_tread = threading.Thread(target=data_ts_finance_indicator_to_mongodb.runallstock, args=(pro, db))
    fin_indicator_tread.start()

    report_income = threading.Thread(target=data_ts_finance_report_to_mongodb.runallstock, args=(pro, db, 'income', 'stock_report_income'))
    report_balancesheet = threading.Thread(target=data_ts_finance_report_to_mongodb.runallstock, args=(pro, db, 'balancesheet', 'stock_report_balancesheet'))
    report_cashflow = threading.Thread(target=data_ts_finance_report_to_mongodb.runallstock, args=(pro, db, 'cashflow', 'stock_report_cashflow'))

    report_income.start()
    report_balancesheet.start()
    report_cashflow.start()


    """
    t_end = datetime.datetime.now()
    print("kongo程序结束时间：{0}".format(str(t_end)))
    print("kongo程序用时：{0}".format(t_end-t_start))
    print('---kongo数据下载结束---')
