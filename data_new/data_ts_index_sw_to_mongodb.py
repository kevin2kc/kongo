import pandas as pd
import datetime
import dataconnection as dc


def loadindexclassifyfromtushare(pro):

    df = pd.DataFrame()
    try:
        df = df = pro.index_classify(fileds="index_code,industry_name, level")
    except Exception as exp:
        print(exp)

    return df


def loadindexmemberfromtushare(pro, index_code):

    df = pd.DataFrame()
    try:
        df = pro.index_member(index_code=index_code,
                              fields="index_code, index_name, con_code, con_name, in_date, out_date, is_new")
    except Exception as exp:
        print(exp)

    return df


def saveindexclassifytomongo(db, data, col_name):
    try:

        result = data.to_dict(orient='records')
        for item in result:
            db[col_name].update_one(item, {'$set': item}, upsert=True)
    except Exception as exp:
        print(exp)


def saveindexmembertomongo(db, data, col_name):
    try:

        result = data.to_dict(orient='records')

        for item in result:
            db[col_name].update_one(item, {'$set': item}, upsert=True)
    except Exception as exp:
        print(exp)


def runallstock(pro, db, classify_col_name, member_col_name):

    print('---开始下载日历数据---')
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))

    # 从tushare读取基础数据
    data1 = loadindexclassifyfromtushare(pro)
    # 如果数据不为空，保存数据到mongo
    saveindexclassifytomongo(db, data1, classify_col_name)

    index_pool = data1['index_code'].tolist()

    # 遍历所有股票
    for i, index in enumerate(index_pool):
        print("程序读取第{0}条编号{1}的数据，总共有{2}".format(i, index, len(index_pool)))
        data2 = loadindexmemberfromtushare(pro, index)
        # 如果数据不为空，保存数据到mongo
        saveindexmembertomongo(db, data2, member_col_name)

    t_end = datetime.datetime.now()
    print("程序结束时间：{0}".format(str(t_end)))
    print("程序用时：{0}".format(t_end-t_start))
    print('---日历数据下载结束---')


def main():
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    print("程序开始时间：{0}".format(str(datetime.datetime.now())))

    # 连接mangoDB
    db = dc.Connection().getmongoconnection()
    # 连接tushare
    pro = dc.Connection().gettushareconnection()

    # 运行主程序
    runallstock(pro, db, "index_sw_classify", "index_sw_member")

    print("程序结束时间：{0}".format(str(datetime.datetime.now())))


if __name__ == '__main__':
    main()
