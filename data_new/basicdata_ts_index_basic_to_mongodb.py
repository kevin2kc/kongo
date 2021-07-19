import pandas as pd
import datetime
import dataconnection as dc


def loadindexbasicfromtushare(pro, market_name):

    df = pd.DataFrame()
    try:
        df = pro.index_basic(market=market_name)
    except Exception as exp:
        print(exp)

    return df


def saveindexbasictomongo(db, data, col_name):
    try:

        result = data.to_dict(orient='records')
        for item in result:
            db[col_name].update_one(item, {'$set': item}, upsert=True)
    except Exception as exp:
        print(exp)


def runallstock(pro, db, col_name, market_list):

    print('---开始下载日历数据---')
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))

    for market in market_list:
        # 从tushare读取index基础数据
        data1 = loadindexbasicfromtushare(pro, market)
        # 如果数据不为空，保存数据到mongo
        print("正在写入{0}市场的数据...".format(market))
        saveindexbasictomongo(db, data1, col_name)
        # 获取所有index列表

    t_end = datetime.datetime.now()
    print("程序结束时间：{0}".format(str(t_end)))
    print("程序用时：{0}".format(t_end-t_start))
    print('---日历数据下载结束---')


def main():
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    print("程序开始时间：{0}".format(str(datetime.datetime.now())))

    con = dc.Connection()
    # 连接mangoDB
    db = con.getmongoconnection()
    # 连接tushare
    pro = con.gettushareconnection()

    # 运行主程序
    runallstock(pro, db, "index_basic", ["SSE", "SZSE"])

    print("程序结束时间：{0}".format(str(datetime.datetime.now())))


if __name__ == '__main__':
    main()
