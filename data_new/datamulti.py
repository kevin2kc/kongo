import threading
import dataconnection

import data_ts_stock_daily_to_mongodb
import data_ts_finance_indicator_to_mongodb

if __name__ == '__main__':

    # 连接mangoDB
    db = dataconnection.mongodbconnection()

    # 连接tushare
    pro = dataconnection.tushareconnection()


    """
    t1 = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'wfq', 'stock_all_wfq'))
    t2 = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'hfq', 'stock_all_hfq'))
    t3 = threading.Thread(target=data_ts_stock_daily_to_mongodb.runallstock, args=(pro, db, 'qfq', 'stock_all_qfq'))


    t1.start()
    t2.start()
    t3.start()
    """

    fin_indicator_tread = threading.Thread(target=data_ts_finance_indicator_to_mongodb.runallstock, args=(pro, db))
    fin_indicator_tread.start()