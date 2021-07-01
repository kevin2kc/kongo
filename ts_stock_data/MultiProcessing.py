import threading

# import ts_stock_daily_hfq_onetable_to_mongodb
# import ts_stock_daily_qfq_onetable_to_mongodb
from data_new import data_ts_stock_daily_to_mongodb

if __name__ == '__main__':
    t1 = threading.Thread(target=data_ts_stock_daily_to_mongodb.main)
    # t2 = threading.Thread(target=ts_stock_daily_hfq_onetable_to_mongodb.main)
    # t3 = threading.Thread(target=ts_stock_daily_qfq_onetable_to_mongodb.main)

    t1.start()
    # t2.start()
    # t3.start()
