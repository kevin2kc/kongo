# 股票基础信息下载 stock_basic
import pandas as pd
import datetime
import dataconnection as dc


def getparameterfromexcel(filename, column_name):

    data = pd.read_excel(filename, engine="openpyxl")
    fields = data[column_name].tolist()
    field_str = ''
    for field in fields:
        field_str = field_str + field + ","
    field_str = field_str[0:len(field_str) - 1]
    return field_str


def loadstockbasicfromtushare(pro):

    field_str = getparameterfromexcel("ts_stock_basic_all_fields.xlsx", "name")

    df = pd.DataFrame()
    try:
        df = pro.stock_basic(exchange='', fields=field_str)
    except Exception as exp:
        print(exp)

    return df


def savestockbasictomongo(db, data, col_name):
    try:

        result = data.to_dict(orient='records')
        for item in result:
            db[col_name].update_one(item, {'$set': item}, upsert=True)

    except Exception as exp:
        print(exp)


def runallstock(pro, db, col_name):

    print('---开始下载股票数据---')
    t_start = datetime.datetime.now()
    print("程序开始时间：{0}".format(str(t_start)))
    # 原始基础数据删除

    # 从tushare读取基础数据

    data = loadstockbasicfromtushare(pro)

    savestockbasictomongo(db, data, col_name)

    # ========================================
    t_end = datetime.datetime.now()
    print("程序结束时间：{0}".format(str(t_end)))
    print("程序用时：{0}".format(t_end-t_start))
    print('---股票数据下载结束---')


def main():
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    print("程序开始时间：{0}".format(str(datetime.datetime.now())))

    # 连接mangoDB
    db = dc.Connection().getmongoconnection()
    # 连接tushare
    pro = dc.Connection().gettushareconnection()

    # 运行主程序
    runallstock(pro, db, 'stock_basic')

    print("程序结束时间：{0}".format(str(datetime.datetime.now())))


if __name__ == '__main__':
    main()
