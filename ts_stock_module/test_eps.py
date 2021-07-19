import cmath
import math

import numpy as np
import pandas as pd

import kongo

def recent_eps(df_eps):

    dict_df = dict(df_eps)

    for k in list(dict_df.keys()):
        if math.isnan((dict_df[k])):
            del dict_df[k]
            continue

    sort_dict = dict(sorted(dict_df.items(), key=lambda x: x[0], reverse=True))

    for key in sort_dict:

        if key.endswith("1231"):
            return sort_dict[key]
        elif key.endswith("0930"):
            return sort_dict[key] * 4/3
        elif key.endswith("0630"):
            return sort_dict[key] * 2
        elif key.endswith("0331"):
            return sort_dict[key] * 4
        else:
            return np.nan



if __name__ == '__main__':

    kon = kongo.Kongo()
    start_date, end_date = '20190101', '20210630'

    df_eps = kon.getstockfinindicatorfrommongo("20200101", "20200930", fields='eps')


    eps_pivot = df_eps.pivot_table(index='ts_code', columns='end_date', values='eps').copy()

    eps_pivot["eps_2020"] = eps_pivot.apply(lambda x: recent_eps(x), axis=1)

    print(eps_pivot)

