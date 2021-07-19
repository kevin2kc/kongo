import dataconnection


def createflagtomongo(db, ts_code):

    try:

        db.stock_update_flag.update_one({'ts_code': ts_code}, {'$set': {'ts_code': ts_code}}, upsert=True)
                                        # 'stock_all_wfq': "", 'stock_all_qfq': "", 'stock_all_hfq': "",
                                        # 'stock_daily_basic': "", 'index_data': "", 'stock_all_easy': ""}},
                                        # upsert=True)

    except Exception as exp:
        print(exp)


def saveflagtomongo(db, ts_code):

    try:

        db.stock_update_flag.insert_one({"ts_code": ts_code})

    except Exception as exp:
        print(exp)


def runallstock():

    con = dataconnection.Connection()
    db = con.getmongoconnection()
    df_stock = con.getstockbasicallfrommongo()
    list_stock = df_stock['ts_code'].tolist()

    createflagtomongo(db, list_stock)
    # date = pd.date_range('2021-01-02', freq='D', periods=30)
    for i, stock in enumerate(list_stock):
        createflagtomongo(db, stock)


if __name__ == '__main__':
    runallstock()
