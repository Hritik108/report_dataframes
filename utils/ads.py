"""Common utility functions for ads"""

import math


def cap(df, cap_col, ads_alloc_col, ads_spend_alloc_col, cost_per_ad):
    df.loc[((df[cap_col] <= df[ads_alloc_col]) & (~df.min_ads_allocated)), ads_alloc_col] = \
        df.loc[((df[cap_col] <= df[ads_alloc_col]) & (~df.min_ads_allocated)), cap_col]
    df.loc[((df[cap_col] <= df[ads_alloc_col]) & (~df.min_ads_allocated)), ads_spend_alloc_col] = \
        df.loc[((df[cap_col] <= df[ads_alloc_col]) & (~df.min_ads_allocated)), ads_alloc_col] * cost_per_ad
    return df


def tag_min_ads_allocation_res_id(res_df, min_ads_allocation, cost_per_ad):
    """
    :param res_df: dataframe containing all the restaurants
    :param min_ads_allocation: minimum number of ads need to be allocated to all stores
    :param cost_per_ad: cost per advertisement
    :return: dataframe with updated ads allocation after comparison with minimum ads allocation
    """
    res_df['min_ads_allocated'] = False
    res_df.loc[res_df['ads_allocated'] <= min_ads_allocation, 'min_ads_allocated'] = True
    res_df.loc[res_df['min_ads_allocated'], 'ads_allocated'] = min_ads_allocation
    res_df.loc[res_df['min_ads_allocated'], 'ads_spend_allocation'] = min_ads_allocation * cost_per_ad
    return res_df


def roundup(x, budget_flexibility):
    if budget_flexibility:
        return round(x, -2)
    else:
        return int(math.floor(x / 100.0)) * 100
