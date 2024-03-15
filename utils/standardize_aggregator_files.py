"""
This util files contains functions related to standardization of aggregator files
"""
import io
import itertools
import os
import re
import pandas as pd
import numpy as np
import json

from utils.exceptions import ColumnMissingException, TabMissingException, MultipleTabsInReviewFile, \
    DBExecutionException, MasterDataNotDefined
from utils.constants import Const
from utils.column_aggregation import ColumnAggregation
from utils import exceptions


def swiggy_date(date):
    """
    standardize date of swiggy of yyyy-mm-dd to yyyymmdd format
    :param date: swiggy date
    :return: dataframe with standardized column names
    """
    date = date.split(' ')[0]
    date = date.split('-')
    date = date[0] + date[1] + date[2]
    return date


def standardize_review_file(review_file, column_mapping, platform):
    """
    standardize review file coming in to be standardized
    :param review_file: review file to be standardized
    :param column_mapping: reference column mapping for standardization
    :param platform: swiggy/zomato
    :return: dataframe with standardized columns
    """
    review_file_dict = {sh: review_file.parse(sh) for sh in review_file.sheet_names}

    if len(review_file_dict) == 0:
        raise TabMissingException("Review data missing", 400)
    if len(review_file_dict) > 1:
        raise MultipleTabsInReviewFile(Const.MULTIPLE_TABS_IN_REVIEW_FILE, 400)

    for sh in review_file_dict:
        data = review_file_dict[sh]

    data = standardize_col_names(data, column_mapping)

    mandatory_cols = ['order_rating', 'comments', 'res_id', 'order_id', 'period']
    missing_mand_cols = list(set(mandatory_cols).difference(set(data.columns)))
    if len(missing_mand_cols) > 0:
        raise ColumnMissingException(f"following columns are missing in file: {', '.join(missing_mand_cols)}", 400)

    data = data[mandatory_cols]

    if platform == 'swiggy':
        data['period'] = data['period'].apply(lambda x: swiggy_date(str(x)))

    data['comments'] = data['comments'].replace(np.nan, '-')
    data['order_rating'] = data['order_rating'].replace(np.nan, 0)
    data = data.apply(lambda x: x.astype(str).str.lower())
    data["res_id"] = pd.to_numeric(data["res_id"])
    return data


def standardize_zomato_file(zomato_xl, column_mapping, logger, objective="ads_optimization", mandatory_tabs=None):
    """
    Function to standardize zomato file
    :param zomato_xl: xlsx file with zomato data
    :param column_mapping: reference column mapping for standardization
    :param logger: logger object for logging
    :param mandatory_tabs: list of tabs mandatory for zomato_file
    :param objective: objective for standardization of file can be ads_optimization/visualization
    :return: standardized zomato dataframe
    """
    if mandatory_tabs is None:
        mandatory_tabs = ['txn_metrics', 'ads', 'funnel', 'grid']

    join_type_ads = join_type_standard = 'outer'
    if objective == 'ads_optimization':
        join_type_ads = 'left'
        join_type_standard = 'inner'

    # getting all dataframes in dict
    zomato_dict = {sh: zomato_xl.parse(sh) for sh in zomato_xl.sheet_names}
    err_str = []

    tabs = {
        'txn_metrics': 'tm',
        'pro': 'pro',
        'new_user': 'nu',
        'funnel': 'funnel',
        'grid': 'grid',
        'ads': 'ads',
        'ors': 'ors',
        'promo': 'promo',
        'for': 'for'
    }
    present_tabs = [tab.lower().strip().replace(' ', '_') for tab in zomato_dict.keys()]
    missing_mand_tabs = [i for i in mandatory_tabs if i not in present_tabs]

    if len(missing_mand_tabs) > 0:
        raise TabMissingException(f"following tabs are missing in file: {', '.join(missing_mand_tabs)}", 400)

    zomato_data = {}
    # creating separate dataframe for each
    for k in zomato_dict.keys():
        df_name = k.lower().replace('data', '').strip().replace(' ', '_')
        if df_name in tabs.keys():
            zomato_dict[k] = standardize_col_names(zomato_dict[k], column_mapping, df_name, f"_{tabs.get(df_name)}")
            zomato_data[df_name] = zomato_dict[k]

    column_aggregation = ColumnAggregation['zomato']

    # TODO: Need to handle this in better way to support weekly and daily aggregation level

    if all(x in zomato_data["txn_metrics"].columns for x in ['res_id_tm', 'period_tm']):
        zomato_data["txn_metrics"]["period_tm"] = zomato_data["txn_metrics"].period_tm.dt.strftime("%Y-%m")
        txn_agg_params = column_aggregation["txn_metrics"]
        all_cols = list(txn_agg_params.keys())
        for key in all_cols:
            if key not in zomato_data["txn_metrics"].columns:
                txn_agg_params.pop(key)
        zomato_data["txn_metrics"] = zomato_data["txn_metrics"].groupby(by=(
            ['res_id_tm', 'period_tm']), as_index=False).agg(txn_agg_params)
    else:
        mandatory_cols = ['res_id_tm', 'period_tm']
        missing_mand_cols = list(set(mandatory_cols).difference(set(zomato_data['txn_metrics'].columns)))
        if len(missing_mand_cols) > 0:
            raise ColumnMissingException(f"following columns are missing in file: {', '.join(missing_mand_cols)}",
                                         400)
    logger.info("txn metrics data aggregated")

    if all(x in zomato_data["funnel"].columns for x in ['res_id_funnel', 'period_funnel']):
        zomato_data["funnel"]["period_funnel"] = zomato_data["funnel"].period_funnel.dt.strftime("%Y-%m")
        funnel_agg_params = column_aggregation["funnel"]
        all_cols = list(funnel_agg_params.keys())
        for key in all_cols:
            if key not in zomato_data["funnel"].columns:
                funnel_agg_params.pop(key)
        zomato_data["funnel"] = zomato_data["funnel"].groupby(by=(['res_id_funnel', 'period_funnel']),
                                                              as_index=False).agg(funnel_agg_params)
    else:
        mandatory_cols = ['res_id_funnel', 'period_funnel']
        missing_mand_cols = list(set(mandatory_cols).difference(set(zomato_data['funnel'].columns)))
        if len(missing_mand_cols) > 0:
            raise ColumnMissingException(f"following columns are missing in file: {', '.join(missing_mand_cols)}",
                                         400)

    logger.info("funnel data aggregated")

    # merging txn metrics and funnel
    zomato_processed = pd.merge(zomato_data["txn_metrics"], zomato_data["funnel"], how=join_type_standard,
                                left_on=['res_id_tm', 'period_tm'],
                                right_on=['res_id_funnel', 'period_funnel'],
                                suffixes=('_tm', '_funnel'))
    logger.info("merged txn_metrics and funnel data")

    # adding grid info
    if "grid" in zomato_data.keys():
        if all(x in zomato_data["grid"].columns for x in ['res_id_grid', 'period_grid']):
            grid_agg_params = column_aggregation["grid"]
            all_cols = list(grid_agg_params.keys())
            for key in all_cols:
                if key not in zomato_data["grid"].columns:
                    grid_agg_params.pop(key)
            zomato_data["grid"]["period_grid"] = zomato_data["grid"].period_grid.dt.strftime("%Y-%m")
            zomato_data["grid"] = zomato_data["grid"].groupby(by=(['res_id_grid', 'period_grid']),
                                                              as_index=False).agg(grid_agg_params)
            logger.info("grid data aggregated")
            zomato_processed = pd.merge(zomato_processed, zomato_data["grid"], how=join_type_standard,
                                        left_on=['res_id_tm', 'period_tm'],
                                        right_on=['res_id_grid', 'period_grid'],
                                        suffixes=('', '_grid'))
            logger.info("merged grid data")
        else:
            err_str.append("res_id or period_id missing in grid data")
    else:
        err_str.append("grid data missing in the input")

    if objective != "ads_optimization":
        # adding pro info
        if "pro" in zomato_data.keys():
            if all(x in zomato_data["pro"].columns for x in ['res_id_pro', 'period_pro']):
                agg_params = column_aggregation["pro"]
                all_cols = list(agg_params.keys())
                for key in all_cols:
                    if key not in zomato_data["pro"].columns:
                        agg_params.pop(key)
                zomato_data["pro"]["period_pro"] = zomato_data["pro"].period_pro.dt.strftime("%Y-%m")
                zomato_data["pro"] = zomato_data["pro"].groupby(by=(['res_id_pro', 'period_pro']),
                                                                as_index=False).agg(agg_params)
                zomato_processed = pd.merge(zomato_processed, zomato_data["pro"], how=join_type_standard,
                                            left_on=['res_id_tm', 'period_tm'],
                                            right_on=['res_id_pro', 'period_pro'],
                                            suffixes=('', '_pro'))
                logger.info("merged pro data")
            else:
                err_str.append("res_id or period_id missing in pro data")
        else:
            err_str.append("pro data missing in the input")

        # adding new user info
        if "new_user" in zomato_data.keys():
            if all(x in zomato_data["new_user"].columns for x in ['res_id_nu', 'period_nu']):
                agg_params = column_aggregation["new_user"]
                all_cols = list(agg_params.keys())
                for key in all_cols:
                    if key not in zomato_data["new_user"].columns:
                        agg_params.pop(key)
                zomato_data["new_user"]["period_nu"] = zomato_data["new_user"].period_nu.dt.strftime("%Y-%m")
                zomato_data["new_user"] = zomato_data["new_user"].groupby(by=(['res_id_nu', 'period_nu']),
                                                                          as_index=False).agg(agg_params)
                zomato_processed = pd.merge(zomato_processed, zomato_data["new_user"], how=join_type_standard,
                                            left_on=['res_id_tm', 'period_tm'],
                                            right_on=['res_id_nu', 'period_nu'],
                                            suffixes=('', '_nu'))
                logger.info("merged new user data")
            else:
                err_str.append("res_id or period_id missing in new_user data")
        else:
            err_str.append("new_user data missing in the input")

        # for - aggregation at res level and month
        if "for" in zomato_data.keys():
            if all(x in zomato_data["for"].columns for x in ['res_id_for', 'period_for']):
                zomato_data["for"]["period_for"] = zomato_data["for"].period_for.dt.strftime("%Y-%m")
                agg_params = column_aggregation["for"]
                all_cols = list(agg_params.keys())
                for key in all_cols:
                    if key not in zomato_data["for"].columns:
                        agg_params.pop(key)
                zomato_data["for"] = zomato_data["for"].groupby(by=(['res_id_for', 'period_for']),
                                                                as_index=False).agg(agg_params)

                # adding for info
                zomato_processed = pd.merge(zomato_processed, zomato_data["for"], how=join_type_standard,
                                            left_on=['res_id_tm', 'period_tm'],
                                            right_on=['res_id_for', 'period_for'],
                                            suffixes=('', '_for'))
                logger.info("merged food order ready data")
            else:
                err_str.append("res_id or period_id missing in food order ready data")
        else:
            err_str.append("food order ready data missing in the input")

        if "ors" in zomato_data.keys():
            if all(x in zomato_data["ors"].columns for x in ['res_id_ors', 'period_ors']):
                zomato_data["ors"]["period_ors"] = zomato_data["ors"].period_ors.dt.strftime("%Y-%m")
                # ors - aggregation at res level and month
                agg_params = column_aggregation["ors"]
                all_cols = list(agg_params.keys())
                for key in all_cols:
                    if key not in zomato_data["ors"].columns:
                        agg_params.pop(key)
                zomato_data["ors"] = zomato_data["ors"].groupby(by=(['res_id_ors', 'period_ors']),
                                                                as_index=False).agg(agg_params)

                # adding ors info
                zomato_processed = pd.merge(zomato_processed, zomato_data["ors"], how=join_type_standard,
                                            left_on=['res_id_tm', 'period_tm'],
                                            right_on=['res_id_ors', 'period_ors'],
                                            suffixes=('', '_ors'))
                logger.info("merged ors data")
            else:
                err_str.append("res_id or period_id missing in ors data")
        else:
            err_str.append("ors data missing in the input")

        if "promo" in zomato_data.keys():
            if all(x in zomato_data["promo"].columns for x in ['res_id_promo', 'period_promo']):
                zomato_data["promo"]["period_promo"] = zomato_data["promo"].period_promo.dt.strftime("%Y-%m")
                agg_params = column_aggregation["promo"]
                all_cols = list(agg_params.keys())
                for key in all_cols:
                    if key not in zomato_data["promo"].columns:
                        agg_params.pop(key)
                zomato_data["promo"] = zomato_data["promo"].groupby(by=(['res_id_promo', 'period_promo']),
                                                                    as_index=False).agg(agg_params)

                # adding promo info
                zomato_processed = pd.merge(zomato_processed, zomato_data["promo"], how=join_type_standard,
                                            left_on=['res_id_tm', 'period_tm'],
                                            right_on=['res_id_promo', 'period_promo'],
                                            suffixes=('', '_promo'))
                logger.info("merged promo data")
            else:
                err_str.append("res_id or period_id missing in promo data")
        else:
            err_str.append("promo data missing in the input")

    if "ads" in zomato_data.keys():
        # ads - aggregation at res level and month
        if all(x in zomato_data["ads"].columns for x in ['res_id_ads', 'period_ads']):
            agg_params = column_aggregation["ads"]
            all_cols = list(agg_params.keys())
            for key in all_cols:
                if key not in zomato_data["ads"].columns:
                    agg_params.pop(key)
            zomato_data["ads"]["period_ads"] = zomato_data["ads"].period_ads.dt.strftime("%Y-%m")
            zomato_data['ads'] = zomato_data["ads"].groupby(by=(['res_id_ads', 'period_ads']), as_index=False).agg(
                agg_params)
        else:
            mandatory_cols = ['res_id_ads', 'period_ads']
            missing_mand_cols = list(set(mandatory_cols).difference(set(zomato_data['ads'].columns)))
            if len(missing_mand_cols) > 0:
                raise ColumnMissingException(f"following columns are missing in file: {', '.join(missing_mand_cols)}",
                                             400)

        # adding ads info
        zomato_processed = pd.merge(zomato_processed, zomato_data["ads"], how=join_type_ads,
                                    left_on=['res_id_tm', 'period_tm'],
                                    right_on=['res_id_ads', 'period_ads'],
                                    suffixes=('', '_ads'))
        logger.info("merged ads data")
    else:
        err_str.append("ads data missing in the input")

    zomato_data, modified = validate_and_add_res_id_period_comb(zomato_data, tabs, logger)

    # adding calculated columns
    mandatory_cols = ['orders_funnel', 'menu_opens_funnel']
    if "ads" in zomato_data.keys():
        mandatory_cols = mandatory_cols + ['sales_generated_ads', 'ads_consumed_ads', 'inorganic_menu_opens_ads',
                                           'ad_impression_ads', 'ad_orders_ads']
    missing_mand_cols = list(set(mandatory_cols).difference(set(zomato_processed.columns)))
    if len(missing_mand_cols) > 0:
        raise ColumnMissingException(f"following columns are missing in file: {', '.join(missing_mand_cols)}", 400)

    # zomato_processed['mto'] = zomato_processed['orders_funnel']/zomato_processed['menu_opens_funnel']
    # zomato_processed['roi'] = zomato_processed['sales_generated_ads']/zomato_processed['ads_consumed_ads']
    # zomato_processed['ctr'] = zomato_processed['clicks_delivered_ads']/zomato_processed['ads_consumed_ads']
    # zomato_processed['ads_cto'] = zomato_processed['ad_orders_ads']/zomato_processed['clicks_delivered_ads']
    # logger.info("added calculated columns")

    merge_res_id_str = " or ".join(['x["' + g + '"]' for g in zomato_processed.filter(like='res_id').columns.tolist()])
    merge_period_str = " or ".join(['x["' + g + '"]' for g in zomato_processed.filter(like='period').columns.tolist()])
    # merge_brand_str = " or ".join(['x["' + g + '"]' for g in zomato_processed.filter(
    #     like='res_name').columns.tolist()])
    merge_city_str = " or ".join(['x["' + g + '"]' for g in zomato_processed.filter(
        like='city').columns.tolist()])
    # merge_sub_zone_str = " or ".join(['x["' + g + '"]' for g in zomato_processed.filter(
    #     like='sub_zone').columns.tolist()])
    for ac in zomato_processed.filter(like='period').columns.tolist():
        zomato_processed[ac] = zomato_processed[ac].astype(object).where(zomato_processed[ac].notnull(), None).fillna(
            '')
    zomato_processed['res_id'] = eval(f'zomato_processed.fillna("").apply(lambda x: ({merge_res_id_str}), axis=1)')
    zomato_processed['period'] = eval(f'zomato_processed.fillna("").apply(lambda x: ({merge_period_str}), axis=1)')
    # zomato_processed['brand'] = eval(f'zomato_processed.fillna("").apply(lambda x: ({merge_brand_str}), axis=1)')
    # zomato_processed['city'] = eval(f'zomato_processed.fillna("").apply(lambda x: ({merge_city_str}), axis=1)')
    # zomato_processed['sub_zone'] = eval(
    #     f'zomato_processed.fillna("").apply(lambda x: ({merge_sub_zone_str}), axis=1)')

    rearrange_cols = ['res_id', 'period', 'rejection',
                      'mto', 'orders_tm', 'aov_tm', 'total_user_tm', 'new_user_tm', 'ctr', 'repeat_user_tm',
                      'um_orders_tm', 'mm_orders_tm', 'roi', 'la_orders_tm', 'kpt_actual_new_tm', 'rejection_tm',
                      'timeouts_tm', 'mx_rejection_tm', 'salt_tm', 'mvd_tm', 'inorganic_m2o', 'subtotal_tm', 'pc_tm',
                      'commissionable_amt_tm', 'pro_discount_tm', 'food_rating_tm', 'delivery_rating_tm', 'adt_tm',
                      'acceptance_time_tm', 'rider_wait_time_tm', 'food_order_ready_at_tm', 'total_rejects_subtotal_tm',
                      'mx_rejects_subtotal_tm', 'discounted_orders_tm', 'overall_discount_value_tm',
                      'cart_built_funnel', 'orders_funnel', 'actuals_grid', 'expected_grid', 'menu_opens_funnel',
                      'grid_visibility_grid', 'weighted_visibility_grid', 'orders_pro',
                      'users_pro', 'commissionable_value_pro', 'pro_discount_pro', 'asv_pro', 'breakfast_orders_pro',
                      'lunch_orders_pro', 'evening_orders_pro', 'dinner_orders_pro', 'late_night_orders_pro',
                      'overall_discount_value_pro', 'merchant_discount_value_pro', 'order_acceptance_time_pro',
                      'delivery_time_pro', 'res_new_user_nu', 'orders_for', 'primary_cuisine_tm',
                      'for_accuracy_new_for', 'for_compliance_for', 'comp_for', 'acc_for', 'orders_ors', 'ors_ors',
                      'poor_quality_ors', 'order_status_delay_ors', 'missing_items_ors', 'wrong_order_ors',
                      'order_cancellation_ors', 'order_spilled_ors', 'instructions_not_followed_ors',
                      'instructions_ors', 'untagged_ors', 'others_ors', 'promo_orders_promo', 'mvd_promo',
                      'promo_orders_subtotal_promo', 'ad_impression_ads', 'inorganic_menu_opens_ads', 'ad_orders_ads',
                      'sales_generated_ads', 'ads_consumed_ads', 'ads_new_users_ads', 'cart_built_ads']
    rearrange_cols = [i for i in rearrange_cols if i in zomato_processed.columns]
    zomato_processed['mx_rejection_tm'] = zomato_processed['mx_rejection_tm'].astype(int)
    zomato_processed['timeouts_tm'] = zomato_processed['timeouts_tm'].astype(int)
    zomato_processed = zomato_processed[rearrange_cols]
    zomato_processed['rejection'] = zomato_processed['mx_rejection_tm'] + zomato_processed['timeouts_tm']
    if 'grid_visibility_grid' not in zomato_processed.columns:
        if ('actuals_grid' in zomato_processed.columns) and ('expected_grid' in zomato_processed.columns):
            zomato_processed['grid_visibility_grid'] = \
                zomato_processed['actuals_grid'] / zomato_processed['expected_grid']
    logger.info(err_str)
    return zomato_processed, zomato_data, modified, err_str


def standardize_swiggy_file(swiggy_xl, column_mapping, logger, objective="ads_optimization", mandatory_tabs=None):
    """
    Function to standardize swiggy file
    :param swiggy_xl: xlsx file with swiggy data
    :param column_mapping: reference column mapping for the standardization
    :param logger: logger object for logging
    :param objective: objective for standardization of file can be ads_optimization/visualization
    :param mandatory_tabs: list of tabs mandatory for swiggy_file
    :return: standardized zomato dataframe
    """
    # getting all dataframes in dict
    if mandatory_tabs is None:
        mandatory_tabs = ['raw_data', 'ads']
    swiggy_dict = {sh.lower().strip().replace(' ', '_'): swiggy_xl.parse(sh) for sh in swiggy_xl.sheet_names}
    err_str = []

    column_aggregation = ColumnAggregation["swiggy"]

    join_type = "outer"
    if objective == "ads_optimization":
        join_type = "left"

    present_tabs = [tab for tab in swiggy_dict.keys()]
    missing_mand_tabs = [i for i in mandatory_tabs if i not in present_tabs]
    if len(missing_mand_tabs) > 0:
        raise TabMissingException(f"following tabs are missing in file: {', '.join(missing_mand_tabs)}", 400)

    # creating separate dataframe for each
    for k in present_tabs:
        df_name = k
        if df_name in ['raw_data', 'ads']:
            if df_name == 'raw_data':
                swiggy_dict[k] = validate_raw_data_header(swiggy_dict[k])
            swiggy_dict[k] = swiggy_dict[k].dropna(axis=1, how='all')
            swiggy_dict[k] = swiggy_dict[k][swiggy_dict[k].columns.dropna()]
            swiggy_dict[k].columns = swiggy_dict[k].columns.str.lower()
            if df_name == "raw_data":
                # remove junk data
                cols_to_drop = list(swiggy_dict[k].filter(regex='Unnamed')) + ['Swiggy']
                cols_to_drop = [col for col in cols_to_drop if col in swiggy_dict[k].columns]
                swiggy_dict[k] = swiggy_dict[k][swiggy_dict[k].columns.drop(cols_to_drop)]
            swiggy_dict[k] = standardize_col_names(swiggy_dict[k], column_mapping, df_name, f"_{df_name}")
            swiggy_dict[k].replace("%", "", inplace=True)

    # Standardize period in ads and raw_data sheet to merge the two sheets accordingly
    swiggy_dict["raw_data"]["period_raw_data"] = swiggy_dict["raw_data"].period_raw_data.dt.strftime("%m-%Y")

    agg_params = column_aggregation["raw_data"]
    all_cols = list(agg_params.keys())
    for key in all_cols:
        if key not in swiggy_dict["raw_data"].columns:
            agg_params.pop(key)
    if all(x in swiggy_dict["raw_data"].columns for x in ['res_id_raw_data', 'period_raw_data']):
        swiggy_dict["raw_data"] = swiggy_dict["raw_data"].groupby(by=(
            ['res_id_raw_data', 'period_raw_data']), as_index=False).agg(agg_params)
    else:
        mandatory_cols = ['res_id_raw_data', 'period_raw_data']
        missing_mand_cols = list(set(mandatory_cols).difference(set(swiggy_dict['raw_data'].columns)))
        if len(missing_mand_cols) > 0:
            raise ColumnMissingException(f"following columns are missing in file: {', '.join(missing_mand_cols)}",
                                         400)

    if "ads" in present_tabs:
        swiggy_dict["ads"]["period_ads"] = swiggy_dict["ads"].period_ads.dt.strftime("%m-%Y")
        swiggy_dict["ads"] = standardize_col_dtypes(swiggy_dict["ads"])

        # Aggregating ads data
        agg_params = column_aggregation["ads"]
        all_cols = list(agg_params.keys())
        for key in all_cols:
            if key not in swiggy_dict["ads"].columns:
                agg_params.pop(key)
        if all(x in swiggy_dict["ads"].columns for x in ['res_id_ads', 'period_ads']):
            swiggy_dict["ads"] = swiggy_dict["ads"].groupby(by=(
                ['res_id_ads', 'period_ads']), as_index=False).agg(agg_params)
        else:
            mandatory_cols = ['res_id_ads', 'period_ads']
            missing_mand_cols = list(set(mandatory_cols).difference(set(swiggy_dict['ads'].columns)))
            if len(missing_mand_cols) > 0:
                raise ColumnMissingException(f"following columns are missing in file: {', '.join(missing_mand_cols)}",
                                             400)

        # merging raw_data and ads
        swiggy_processed = pd.merge(swiggy_dict["raw_data"], swiggy_dict["ads"], how=join_type,
                                    left_on=['res_id_raw_data', 'period_raw_data'],
                                    right_on=['res_id_ads', 'period_ads'],
                                    suffixes=('_raw_data', '_ads'))
        logger.info("merged ads and raw data")
    else:
        swiggy_processed = swiggy_dict["raw_data"]

    swiggy_dict, modified = validate_and_add_res_id_period_comb(swiggy_dict, {"raw_data": 'raw_data', "ads": "ads"},
                                                                logger)
    logger.info("raw file validated and missing data added")

    missing_mand_cols = []
    if 'm2o_raw_data' in swiggy_processed.columns:
        swiggy_processed["menu_opens_raw_data"] = swiggy_processed["orders_raw_data"] / swiggy_processed["m2o_raw_data"]
    elif 'menu_opens_raw_data' in swiggy_processed.columns:
        swiggy_processed["m2o_raw_data"] = swiggy_processed["orders_raw_data"] / swiggy_processed["menu_opens_raw_data"]
    else:
        missing_mand_cols = ['menu_opens_raw_data/m2o_raw_data']

    mandatory_cols = ['orders_raw_data']
    if "ads" in present_tabs:
        mandatory_cols = mandatory_cols + ['inorganic_menu_opens_ads', 'ad_impression_ads', 'ad_orders_ads',
                                           'sales_generated_ads', 'ads_consumed_ads']

    missing_mand_cols = missing_mand_cols + list(set(mandatory_cols).difference(swiggy_processed.columns))
    if len(missing_mand_cols) > 0:
        raise ColumnMissingException(f"following columns are missing in file: {', '.join(missing_mand_cols)}", 400)

    if 'acceptance_raw_data' in swiggy_processed.columns:
        swiggy_processed["acceptance_raw_data"] = swiggy_processed["acceptance_raw_data"].replace(np.nan, 1)
        swiggy_processed["rejection"] = swiggy_processed["orders_raw_data"] / swiggy_processed["acceptance_raw_data"]
        swiggy_processed["rejection"] = swiggy_processed["rejection"] - swiggy_processed["orders_raw_data"]
        swiggy_processed["rejection"] = swiggy_processed["rejection"].replace([np.nan, np.inf], 0)
        swiggy_processed["rejection"] = swiggy_processed["rejection"].astype(int)
    if ('new_users_overall_raw_data' in swiggy_processed.columns) and \
            ('repeat_user_base_raw_data' in swiggy_processed.columns):
        swiggy_processed["total_user_base"] = \
            swiggy_processed["new_users_overall_raw_data"] + swiggy_processed["repeat_user_base_raw_data"]

    merge_res_id_str = " or ".join(['x["' + g + '"]' for g in swiggy_processed.filter(
        like='res_id').columns.tolist()])
    merge_period_str = " or ".join(['x["' + g + '"]' for g in swiggy_processed.filter(
        like='period').columns.tolist()])
    # merge_brand_str = " or ".join(['x["' + g + '"]' for g in swiggy_processed.filter(
    #     like='res_name').columns.tolist()])
    # merge_city_str = " or ".join(['x["' + g + '"]' for g in swiggy_processed.filter(
    #     like='city').columns.tolist()])
    # merge_sub_zone_str = " or ".join(['x["' + g + '"]' for g in swiggy_processed.filter(
    #     like='sub_zone').columns.tolist()])
    for ac in swiggy_processed.filter(like='period').columns.tolist():
        swiggy_processed[ac] = swiggy_processed[ac].astype(object).where(
            swiggy_processed[ac].notnull(), None).fillna('')
    swiggy_processed['res_id'] = eval(f'swiggy_processed.fillna("").apply(lambda x: ({merge_res_id_str}), axis=1)')
    swiggy_processed['period'] = eval(f'swiggy_processed.fillna("").apply(lambda x: ({merge_period_str}), axis=1)')
    # swiggy_processed['brand'] = eval(f'swiggy_processed.fillna("").apply(lambda x: ({merge_brand_str}), axis=1)')
    # swiggy_processed['city'] = eval(f'swiggy_processed.fillna("").apply(lambda x: ({merge_city_str}), axis=1)')
    # swiggy_processed['sub_zone'] = eval(
    #     f'swiggy_processed.fillna("").apply(lambda x: ({merge_sub_zone_str}), axis=1)')

    rearrange_cols = ['res_id', 'period', 'aov_raw_data', 'orders_raw_data', 'sales_generated_raw_data',
                      'mark_food_ready_accuracy_raw_data', 'mark_food_ready_adoption_raw_data', 'acceptance_raw_data',
                      'restaurant_cancellations_raw_data', 'edits_raw_data', 'igcc_raw_data', 'prep_time_raw_data',
                      'grid_visibility_raw_data', 'average_food_ratings_raw_data', 'new_user_orders_raw_data',
                      'new_users_overall_raw_data', 'repeat_user_orders_raw_data', 'repeat_user_base_raw_data',
                      'overall_repeat_rate_raw_data', 'p1_customers_raw_data', 'p2_customers_raw_data',
                      'p3_customers_raw_data', 'unclassified_raw_data', 'ad_impression_raw_data', 'menu_opens_raw_data',
                      'l2m_raw_data', 'm2c_raw_data', 'c2p_raw_data', 'p2o_raw_data', 'mto_raw_data',
                      'cart_builds_raw_data', 'bad_order_raw_data', 'total_food_issue_raw_data',
                      'quality_issue_raw_data', 'quantity_issue_raw_data', 'packaging_raw_data', 'location_raw_data',
                      'wrong_item_raw_data', 'special_inst_issue_raw_data', 'missing_item_raw_data',
                      'swiggyit_orders_raw_data', 'swiggyit_burn_raw_data', 'jumbo_orders_raw_data',
                      'jumbo_burn_raw_data', 'party_orders_raw_data', 'b2g1_orders_raw_data',
                      'party_burn_raw_data', 'unlimited_orders_raw_data', 'unlimited_burn_raw_data',
                      'b2g1_burn_raw_data', 'b1g1_orders_raw_data', 'b1g1_burn_raw_data',
                      'dotd_steal_deal_orders_raw_data', 'dotd_steal_deal_burn_raw_data', 'inorganic_menu_opens_ads',
                      'dormant_missed_you_orders_raw_data', 'dormant_missed_you_burn_raw_data',
                      'new_customer_try_new_orders_raw_data', 'new_customer_try_new_burn_raw_data',
                      'swiggy_one_eo_orders_raw_data', 'swiggy_one_eo_burn_raw_data',
                      'ad_orders_ads', 'sub_zone_raw_data', 'ads_consumed_ads', 'ad_impression_ads',
                      'ads_new_users_ads', 'sales_generated_ads', 'm2o_raw_data', 'rejection', 'total_user_base']
    rearrange_cols = [i for i in rearrange_cols if i in swiggy_processed.columns]
    swiggy_processed = swiggy_processed[rearrange_cols]
    return swiggy_processed, swiggy_dict, modified


def validate_raw_data_header(raw_data):
    """
    Function to validate and correct the header of raw_data
    :param raw_data:
    :return: raw_data with correct headers
    """
    initial_header = list(raw_data.columns.str.lower().to_series().filter(regex='^((?!unnamed).)*$'))
    possible_header = pd.Series(list(raw_data.iloc[0])).filter(regex='^((?!unnamed).)*$').dropna()
    if len(possible_header) > len(initial_header):
        headers = raw_data.iloc[0]
        raw_data = pd.DataFrame(raw_data.values[1:], columns=headers)
    return raw_data


def standardize_col_names(df, column_mapping, tab_name='', suffix=''):
    """
    Function to standardize column names of zomato and swiggy dataframe
    :param df: swiggy/zomato dataframe for which column names are to be standardized
    :param column_mapping: reference column mapping for standardization
    :param tab_name: tab_name for customization related to ads
    :param suffix: suffix for adding to column names for respective sheet
    :return: dataframe with standardized column names
    """
    # mapping column names
    if tab_name == 'ads':
        df.rename(
            columns={'budget burnt %': 'budget burnt percentage', 'budget remaining %': 'budget remaining percentage'},
            inplace=True
        )

    df.columns = df.columns.str.lower().str.replace('%', '').str.strip().str.replace(' - ', ' ').str.replace(' ', '_')

    if tab_name == "ads":
        df.rename(columns={'orders': 'orders_ads'}, inplace=True)
        df.rename(columns={'order': 'order_ads'}, inplace=True)
        df.rename(columns={'new_user': 'new_user_ads'}, inplace=True)
        df.rename(columns={'new_users': 'new_users_ads'}, inplace=True)

    str_columns_names = '###'.join([s.strip() for s in df.columns.to_list()])
    str_columns_names = '#' + str_columns_names.lower() + '#'

    for k, v in column_mapping.items():
        if isinstance(v, str):
            v = [v]
        t = '#' + '#|#'.join(v) + '#'
        str_columns_names = re.sub(t.lower(), k, str_columns_names)

    str_columns_names = re.sub('[#]+', '#SEP#', str_columns_names).strip()
    df.columns = [i for i in str_columns_names.split('#SEP#') if i != '']
    df.columns = df.columns.str.lower().str.replace('%', '').str.strip().str.replace(' - ', ' ').str.replace(' ', '_')
    df.columns = eval(f'[n + "{suffix}" for n in df.columns]')
    return df


def standardize_col_dtypes(data):
    """
    This function converts datatype of input data columns to int/float depending on the type of column
    :param data: input data frame
    :return: data frame with column dtypes standardized
    """
    old_cols = data.columns
    data.columns = data.columns.str.replace(r'_(ads)$|_(tm)$|_(funnel)$|_(raw_data)$|_(grid)$', '', regex=True)
    if "orders" in data.columns:
        data["orders"] = data["orders"].astype(int, errors='ignore')
    if "ad_orders" in data.columns:
        data["ad_orders"] = data["ad_orders"].astype(int, errors='ignore')
    if "inorganic_menu_opens" in data.columns:
        data["inorganic_menu_opens"] = data["inorganic_menu_opens"].astype(int, errors='ignore')
    if "ad_impression" in data.columns:
        data["ad_impression"] = data["ad_impression"].astype(int, errors='ignore')
    if "menu_opens" in data.columns:
        data["menu_opens"] = data["menu_opens"].astype(int, errors='ignore')
    if "ads_consumed" in data.columns:
        data["ads_consumed"] = data["ads_consumed"].astype(float, errors='ignore')
    if "rejection" in data.columns:
        data["rejection"] = data["rejection"].astype(float, errors='ignore')
    if "grid_visibility" in data.columns:
        data["grid_visibility"] = data["grid_visibility"].astype(float, errors='ignore')
    if "ads_new_users" in data.columns:
        data["ads_new_users"] = data["ads_new_users"].astype(int, errors='ignore')
    if "new_users_overall" in data.columns:
        data["new_users_overall"] = data["new_users_overall"].astype(int, errors='ignore')
    if "total_user_base" in data.columns:
        data["total_user_base"] = data["total_user_base"].astype(int, errors='ignore')
    if "sales_generated" in data.columns:
        data["sales_generated"] = data["sales_generated"].astype(str).str.replace(
            ",", "").astype(float, errors='ignore')
    if "kpt_actual_new" in data.columns:
        data["kpt_actual_new"] = data["kpt_actual_new"].astype(str).str.replace(
            ",", "").astype(float, errors='ignore')
    data.columns = old_cols
    return data


def standardize_restaurant_master_data(conn, data, client_id, logger, join_type="inner"):
    """
    standardize restaurant master data like brand, city and sub_zone
    :param conn: database connection object
    :param data: reference data for standardization
    :param client_id: int
    :param logger: logger object
    :param join_type: join type outer/inner
    :return: standardized restaurant data if successfully fetched otherwise respective error msg
    """
    query = f"select sub_zone, city, brand, res_id from restaurant_master_data " \
            f"where client_id = {client_id}"
    try:
        result = pd.read_sql(query, conn)
    except:
        logger.error('DB failure while getting restaurant master data')
        raise exceptions.DBExecutionException
    if len(result.index) == 0:
        raise exceptions.MasterDataNotDefined
    result["res_id"] = pd.to_numeric(result["res_id"])
    data = pd.merge(result, data.drop(columns=['sub_zone', 'city', 'brand'], errors='ignore'), how=join_type,
                    on='res_id')
    return data


def validate_and_add_res_id_period_comb(data, tabs, logger):
    """
    Function checks if a pair of res_id and period is present in the data frame or not.
    If not present then it adds the pair to the data frame
    :param data: data frame
    :param tabs: dict of tabs, key is tab name and value is column suffix
    :return: data frame of the Excel sheets and a boolean flag indicating if the data frame is modified or not
    """
    res_list = []
    period_list = []
    modified = False
    logger.info("getting unique period and res_ids")
    for tab in tabs.keys():
        if tab in data.keys():
            period_list.extend(data[tab][f'period_{tabs[tab]}'].unique().tolist())
            res_list.extend(data[tab][f'res_id_{tabs[tab]}'].unique().tolist())
            data[tab]["pairs"] = data[tab][f'res_id_{tabs[tab]}'].astype(str) + "_" + \
                                 data[tab][f'period_{tabs[tab]}'].astype(str)
            data[tab]["comments"] = ""

    res_list = list(set(res_list))
    period_list = list(set(period_list))
    logger.info("getting cross product of period and res_ids")
    res_period_list_temp = list(itertools.product(res_list, period_list))
    res_period_list = []
    logger.info("finished generating cross product of period and res_ids")
    for i in res_period_list_temp:
        res_period_list.append(str(i[0]) + "_" + str(i[1]))

    logger.info("checking missing pairs of res_id and period for individual sheets")
    for tab in tabs.keys():
        is_tab_modified = False
        if tab in data.keys():
            missing_pairs = list(set(res_period_list) - set(data[tab]["pairs"]))
            if len(missing_pairs) > 0:
                modified = True
                is_tab_modified = True
                missing_pairs_df = {
                    f'res_id_{tabs[tab]}': [pair.split("_")[0] for pair in missing_pairs],
                    f'period_{tabs[tab]}': [pair.split("_")[1] for pair in missing_pairs],
                    'comments': ["Add data for this res_id and period"] * len(missing_pairs)
                }
                data[tab] = pd.concat([data[tab], pd.DataFrame(missing_pairs_df)])

            # if is_tab_modified:
            #     data[tab] = data[tab].style.apply(highlight_rows, axis=1)
    return data, modified


# TODO: Optimize this function to reduce time complexity
def highlight_rows(row):
    """
    Function to highlight rows in the data frame
    :param row:
    :return: Highlighted rows
    """
    value = row.loc['comments']
    if value != "":
        color = '#FFFF00'  # Yellow
    else:
        color = '#FFFFFF'  # White
    return ['background-color: {}'.format(color) for cell in row]


def save_suggested_file_to_s3(s3, client_id, transaction_id, raw, destination):
    """
    Function to put the suggested file in the s3 bucket
    :param s3: s3 resource object
    :param client_id: int
    :param transaction_id: int
    :param raw: dict of data frames
    :param destination: destination folder in s3 bucket
    """

    with io.BytesIO() as output:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for key in raw.keys():
                raw[key].to_excel(writer, sheet_name=key, index=False)
        suggested = output.getvalue()
    s3.put_object(Bucket=os.environ.get('DATA_BUCKET'),
                  Key=f'{destination}/{client_id}/{transaction_id}_raw.xlsx',
                  Body=suggested)
