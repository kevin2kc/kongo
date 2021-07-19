import numpy as np
import pandas as pd
import statsmodels.api as sm
import formula_1
import kongo

if __name__ == '__main__':

    kon = kongo.Kongo()
    start_date, end_date = '20200701', '20210630'
    report_date = '20210331'

    df_sk = kon.getstockbasicallfrommongo()
    df_sk_grouped = df_sk.groupby('market')

    bank_list = list(df_sk_grouped.groups.keys())
    bank_list.pop(0)

    index_list = ['399001.SZ', '000001.SH', '399006.SZ', '000688.SH']
    bank_dict = dict(zip(bank_list, index_list))

    with pd.ExcelWriter('pvgo_' + start_date + '_' + end_date + '.xlsx', mode='w', engine="openpyxl") as writer:

        df_result_sum = pd.DataFrame()

        for bank in bank_list:

            df_bank = df_sk_grouped.get_group(bank)
            list_zb = df_bank['ts_code'].tolist()

            df_price = kon.getstockdailyfrommongo(end_date, end_date, list_zb)
            list_zb = df_price['ts_code'].tolist()

            df_stockinfo = kon.getstocktinfo(list_zb, method='ts_code')
            list_zb_name = df_stockinfo['name'].tolist()

            print("计算EPS...")
            df_eps = kon.getstockfinindicatorfrommongo(report_date, report_date, fields='eps')
            df_eps = df_eps.pivot_table(index='ts_code', columns='end_date', values='eps').copy()
            df_eps["eps"] = df_eps.apply(lambda x: formula_1.recent_eps(x), axis=1)
            df_eps.reset_index(inplace=True)
            print("计算EPS完成...")

            df_shibor = kon.getshiborfrommongo(start_date, end_date)
            Rf = round(df_shibor['1y'].mean()/100, 6)

            stock_return, stock_return_mean, stock_return_sum, stock_return_volatility = formula_1.getstockdata(start_date, end_date,
                                                                    ts_codes=list_zb, index_codes=index_list)

            df_result = pd.DataFrame(columns=["ts_code", "name", "pvgo=price-eps/Ra", "price_"+end_date,
                                              "pvgo / price", "eps_recent",
                                              "Ra=Rf+Ba*(Rm-Rf)", "Rf=shibor(risk free rate)", "Rm=(market_return)",
                                              "Ba_from_OLS", "a_from_OLS", "rsquared_adj_from_OLS"])

            for ts_code, name in zip(list_zb, list_zb_name):

                bank_index_addcons = sm.add_constant(stock_return[bank_dict[bank]])
                model_stock_bank = sm.OLS(endog=stock_return[ts_code], exog=bank_index_addcons)
                result_stock_bank = model_stock_bank.fit()

                df_stock_price = df_price[df_price["ts_code"] == ts_code].reset_index()
                stock_price = df_stock_price.loc[0, 'close']

                # eps的信息
                df_stock_eps = df_eps[df_eps["ts_code"] == ts_code].reset_index()
                if not df_stock_eps.empty:
                    stock_eps = df_stock_eps.loc[0, 'eps']
                else:
                    stock_eps = np.nan

                Ra, pvgo_value = np.nan, np.nan

                try:
                    Ra = Rf+result_stock_bank.params[1]*(stock_return_mean[bank_dict[bank]]-Rf)
                    pvgo_value = stock_price - stock_eps / Ra
                except Exception as exp:
                    print("发生错误：{0},{1}".format(ts_code, exp))

                print("股票代码:{0},a的值是{1},B的值是{2},eps的值是{3},Ra的值是{4},pvgo的值是{5}".
                      format(ts_code, result_stock_bank.params[0], result_stock_bank.params[1], stock_eps, Ra, pvgo_value))
                df_result = df_result.append({"ts_code": ts_code, "name": name, "pvgo=price-eps/Ra": pvgo_value,
                                              "price_"+end_date: stock_price, "pvgo / price": pvgo_value/stock_price,
                                              "eps_"+report_date: stock_eps,
                                              "Ra=Rf+Ba*(Rm-Rf)": Ra, "Rf=shibor(risk free rate)": Rf,
                                              "Rm=(market_return)": stock_return_sum[bank_dict[bank]],
                                              "Ba_from_OLS": result_stock_bank.params[1],
                                              "a_from_OLS": result_stock_bank.params[0],
                                              "rsquared_adj_from_OLS": result_stock_bank.rsquared_adj}, ignore_index=True)



            df_result.to_excel(writer, sheet_name=bank, index=False)
            print("股票： {0} 板块结束计算".format(bank))
            df_result_sum = pd.concat([df_result_sum, df_result])

        df_result_sum.to_excel(writer, sheet_name="汇总表", index=False)
