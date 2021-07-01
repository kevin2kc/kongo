import threading
import dataconnection

import data_ts_stock_daily_to_mongodb
import data_ts_finance_indicator_to_mongodb
import data_ts_finance_report_to_mongodb
import data_ts_trade_cal_to_mongodb
import data_ts_daily_basic_to_mongodb
import data_ts_stock_basic_to_mongodb

if __name__ == '__main__':

    # 连接mangoDB
    db = dataconnection.Connection().getmongoconnection()

    # 连接tushare
    pro = dataconnection.Connection().gettushareconnection()

    data_ts_stock_basic_to_mongodb.runallstock(pro, db)
    data_ts_trade_cal_to_mongodb.runallstock(pro, db)

    stock_wfq = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'wfq', 'stock_all_wfq'))
    stock_hfq = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'hfq', 'stock_all_hfq'))
    stock_qfq = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'qfq', 'stock_all_qfq'))

    stock_wfq.start()
    stock_qfq.start()
    stock_hfq.start()

    fin_indicator_tread = threading.Thread(target=data_ts_finance_indicator_to_mongodb.runallstock, args=(pro, db))
    fin_indicator_tread.start()

    report_income = threading.Thread(target=data_ts_finance_report_to_mongodb.runallstock, args=(pro, db, 'income', 'stock_report_income'))
    report_balancesheet = threading.Thread(target=data_ts_finance_report_to_mongodb.runallstock, args=(pro, db, 'balancesheet', 'stock_report_balancesheet'))
    report_cashflow = threading.Thread(target=data_ts_finance_report_to_mongodb.runallstock, args=(pro, db, 'cashflow', 'stock_report_cashflow'))

    report_income.start()
    report_balancesheet.start()
    report_cashflow.start()

    daily_basic = threading.Thread(target=data_ts_daily_basic_to_mongodb.runallstock, args=(pro, db))
    daily_basic.start()
