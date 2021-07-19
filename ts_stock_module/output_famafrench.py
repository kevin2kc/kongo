import numpy as np
import pandas as pd
import kongo
import formula_1

# 账面市值比的大/中/小分类
BM_BIG_MV_BIG = 'BM_BIG_MV_BIG'
BM_MID_MV_BIG = 'BM_MID_MV_BIG'
BM_SMA_MV_BIG = 'BM_SMA_MV_BIG'
BM_BIG_MV_SMA = 'BM_BIG_MV_SMA'
BM_MID_MV_SMA = 'BM_MID_MV_SMA'
BM_SMA_MV_SMA = 'BM_SMA_MV_SMA'

def init(context):
    # 数据滑窗
    context.date = 20
    # 设置开仓最大资金量
    context.ratio = 0.8
    # 账面市值比的大/中/小分类
    context.BM_BIG_MV_BIG = 6
    context.BM_MID_MV_BIG = 5
    context.BM_SMA_MV_BIG = 4
    context.BM_BIG_MV_SMA = 3
    context.BM_MID_MV_SMA = 2
    context.BM_SMA_MV_SMA = 1


def market_value_weighted(stocks, MV, BM):
    select = stocks[(stocks.mv == MV) & (stocks.bm == BM)]
    market_value = select['mv'].values
    mv_total = np.sum(market_value)
    mv_weighted = [mv / mv_total for mv in market_value]
    stock_return = select['return'].vaules

    return_total = []
    for i in range(len(mv_weighted)):
        return_total.append(mv_weighted[i] * stock_return[i])
    return_total = np.sum(return_total)
    return return_total

def algo(context):
    pass

def category_bm_mv(df, size_bm, size_mv):

    if df["bm"] < size_bm[0]:
        if df["mv"] < size_mv:
            df["group"] = BM_SMA_MV_SMA
        else:
            df["group"] = BM_SMA_MV_BIG

    elif df["bm"] > size_bm[1]:
        if df["mv"] < size_mv:
            df["group"] = BM_BIG_MV_SMA
        else:
            df["group"] = BM_BIG_MV_BIG

    else:
        if df["mv"] < size_mv:
            df["group"] = BM_MID_MV_SMA
        else:
            df["group"] = BM_MID_MV_BIG

    return df

def get_index_smb_hml(start_date, end_date):
    con = kongo.Kongo()
    df_bm_mv = con.getstockdailybasicfrommongo(start_date, end_date)
    df_bm_mv.loc[:, "bm"] = df_bm_mv.loc[:, "pb"] ** -1 * df_bm_mv.loc[:, "total_mv"]
    df_bm_mv = df_bm_mv[["ts_code", "bm", "total_mv"]]
    df_bm_mv.rename(columns={"total_mv": "mv"}, inplace=True)
    # df.drop_duplicates(subset=["ts_code"], inplace=True)

    size_bm = df_bm_mv["bm"].quantile([0.3, 0.7]).tolist()
    size_mv = df_bm_mv["mv"].quantile(.5)

    print("市值和账面价值进行分类...")
    df_bm_mv = df_bm_mv.apply(lambda x: category_bm_mv(x, size_bm, size_mv), axis=1)
    print("市值和账面价值分类完成...")
    # 获取股票期间收益
    stock_list = df_bm_mv['ts_code'].tolist()

    print("获取所有股票的期间收益...")
    return_sum = formula_1.getstockreturnsum(start_date, end_date, stock_list)

    # 期间收益表和bm mv表两表合并
    print("合并股票分类和期间收益...")
    df_bm_mv_return = pd.merge(df_bm_mv, return_sum, on='ts_code')

    df_index = pd.DataFrame()

    # 合并后的表按组编码来分组
    df_grouped = df_bm_mv_return.groupby("group")
    df_index_groups = df_grouped.size().index

    print(df_bm_mv_return)

    #
    # for group in df_index_groups:
    #
    #     df_gp_list = df_grouped.get_group(group).copy()
    #     # 获取一个组的全部股票代码, 进行组内指标的日收益计算
    #     df_gp_list["weight"] = df_gp_list["mv"]/df_gp_list["mv"].sum()
    #
    #     print("分组名称为：{0}的数据开始计算...".format(group))
    #     df_result_temp = formula_1.getstockreturnsumbyweight(start_date, end_date,
    #                                    df_gp_list['ts_code'].tolist(), df_gp_list["weight"].tolist())
    #
    #     df_result_temp.rename(columns={"weightsum": group}, inplace=True)
    #
    #     df_index = pd.concat([df_index, df_result_temp], axis=1)
    #     print("分组名称为：{0}的数据计算完成...".format(group))
    #
    # # 小市值的指标
    # df_index["smb_s"] = (df_index["BM_BIG_MV_SMA"] + df_index["BM_MID_MV_SMA"] + df_index["BM_SMA_MV_SMA"]) / 3
    # # 大市值的指标
    # df_index["smb_b"] = (df_index["BM_BIG_MV_BIG"] + df_index["BM_MID_MV_BIG"] + df_index["BM_SMA_MV_BIG"]) / 3
    # df_index["smb"] = df_index["smb_s"] - df_index["smb_b"]
    #
    # # 小账面的指标
    # df_index["hml_s"] = (df_index["BM_SMA_MV_SMA"] + df_index["BM_SMA_MV_BIG"]) / 2
    # # 大账面的指标
    # df_index["hml_b"] = (df_index["BM_BIG_MV_SMA"] + df_index["BM_BIG_MV_BIG"]) / 2
    # df_index["hml"] = df_index["hml_b"] - df_index["hml_s"]
    #
    # return df_index


if __name__ == '__main__':

    get_index_smb_hml("20210101", "20210630")

    # df = get_index_smb_hml("20210101", "20210630")
    # print(df)
    # df.to_excel("index.xlsx", engine="openpyxl")



    # kon = kongo.Kongo()
    # start_date, end_date = '20200701', '20210630'
    # report_date = '20210331'
    #
    # df_sk = kon.getstockbasicallfrommongo()
    # df_sk_grouped = df_sk.groupby('market')
    #
    # bank_list = list(df_sk_grouped.groups.keys())
    # bank_list.pop(0)
    #
    # index_list = ['399001.SZ', '000001.SH', '399006.SZ', '000688.SH']
    # bank_dict = dict(zip(bank_list, index_list))
    #
    # with pd.ExcelWriter('pvgo_' + start_date + '_' + end_date + '.xlsx', mode='w', engine="openpyxl") as writer:
    #
    #     df_result_sum = pd.DataFrame()
    #
    #     for bank in bank_list:
    #
    #         df_bank = df_sk_grouped.get_group(bank)
    #         list_zb = df_bank['ts_code'].tolist()
    #
    #         df_price = kon.getstockdailyfrommongo(end_date, end_date, list_zb)
    #         list_zb = df_price['ts_code'].tolist()
    #
    #         df_stockinfo = kon.getstocktinfo(list_zb, method='ts_code')
    #         list_zb_name = df_stockinfo['name'].tolist()
    #
    #         print("计算EPS...")
    #         df_eps = kon.getstockfinindicatorfrommongo(report_date, report_date, fields='eps')
    #         df_eps = df_eps.pivot_table(index='ts_code', columns='end_date', values='eps').copy()
    #         df_eps["eps"] = df_eps.apply(lambda x: formula_1.recent_eps(x), axis=1)
    #         df_eps.reset_index(inplace=True)
    #         print("计算EPS完成...")
    #
    #         df_shibor = kon.getshiborfrommongo(start_date, end_date)
    #         Rf = round(df_shibor['1y'].mean()/100, 6)
    #
    #         stock_return, stock_return_mean, stock_return_sum, stock_return_volatility = formula_1.getstockdata(start_date, end_date,
    #                                                                 ts_codes=list_zb, index_codes=index_list)
    #
    #         df_result = pd.DataFrame(columns=["ts_code", "name", "pvgo=price-eps/Ra", "price_"+end_date,
    #                                           "pvgo / price", "eps_recent",
    #                                           "Ra=Rf+Ba*(Rm-Rf)", "Rf=shibor(risk free rate)", "Rm=(market_return)",
    #                                           "Ba_from_OLS", "a_from_OLS", "rsquared_adj_from_OLS"])
    #
    #         for ts_code, name in zip(list_zb, list_zb_name):
    #
    #             bank_index_addcons = sm.add_constant(stock_return[bank_dict[bank]])
    #             model_stock_bank = sm.OLS(endog=stock_return[ts_code], exog=bank_index_addcons)
    #             result_stock_bank = model_stock_bank.fit()
    #
    #             df_stock_price = df_price[df_price["ts_code"] == ts_code].reset_index()
    #             stock_price = df_stock_price.loc[0, 'close']
    #
    #             # eps的信息
    #             df_stock_eps = df_eps[df_eps["ts_code"] == ts_code].reset_index()
    #             if not df_stock_eps.empty:
    #                 stock_eps = df_stock_eps.loc[0, 'eps']
    #             else:
    #                 stock_eps = np.nan
    #
    #             Ra, pvgo_value = np.nan, np.nan
    #
    #             try:
    #                 Ra = Rf+result_stock_bank.params[1]*(stock_return_mean[bank_dict[bank]]-Rf)
    #                 pvgo_value = stock_price - stock_eps / Ra
    #             except Exception as exp:
    #                 print("发生错误：{0},{1}".format(ts_code, exp))
    #
    #             print("股票代码:{0},a的值是{1},B的值是{2},eps的值是{3},Ra的值是{4},pvgo的值是{5}".
    #                   format(ts_code, result_stock_bank.params[0], result_stock_bank.params[1], stock_eps, Ra, pvgo_value))
    #             df_result = df_result.append({"ts_code": ts_code, "name": name, "pvgo=price-eps/Ra": pvgo_value,
    #                                           "price_"+end_date: stock_price, "pvgo / price": pvgo_value/stock_price,
    #                                           "eps_"+report_date: stock_eps,
    #                                           "Ra=Rf+Ba*(Rm-Rf)": Ra, "Rf=shibor(risk free rate)": Rf,
    #                                           "Rm=(market_return)": stock_return_sum[bank_dict[bank]],
    #                                           "Ba_from_OLS": result_stock_bank.params[1],
    #                                           "a_from_OLS": result_stock_bank.params[0],
    #                                           "rsquared_adj_from_OLS": result_stock_bank.rsquared_adj}, ignore_index=True)
    #
    #
    #
    #         df_result.to_excel(writer, sheet_name=bank, index=False)
    #         print("股票： {0} 板块结束计算".format(bank))
    #         df_result_sum = pd.concat([df_result_sum, df_result])
    #
    #     df_result_sum.to_excel(writer, sheet_name="汇总表", index=False)
