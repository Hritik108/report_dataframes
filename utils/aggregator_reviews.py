"""
This file contains util functions for aggregator reviews
"""

import numpy as np


def append_stats(data):
    """
    This function is used to get data statistics
    :param data: data
    :return: data statistics
    """

    data['avg_rating'] = data[[1, 2, 3, 4, 5]].apply(lambda x: np.ma.average([1, 2, 3, 4, 5], weights=x), axis=1)
    data['avg_rating'] = data['avg_rating'].round(2)
    data['poor_rating'] = data[[1, 2]].apply(lambda x: np.sum(x), axis=1)
    data['good_rating'] = data[[4, 5]].apply(lambda x: np.sum(x), axis=1)
    data['neutral_rating'] = data[[3]].apply(lambda x: np.sum(x), axis=1)
    data['total_reviews'] = data[[1, 2, 3, 4, 5]].apply(lambda x: np.sum(x), axis=1)

    return data


def standardize_date_format_aggregator_reviews(start_date, end_date):
    """
    This function is used to get the start and end dates as per review aggregation format
    :param start_date: input start date
    :param end_date: input end date
    :return: start and end dates as per review aggregation
    """

    start_date = start_date.strftime("%Y%m%d")
    end_date = end_date.strftime("%Y%m%d")

    return start_date, end_date


def get_filter_string_aggregator_reviews(body):
    """
    Function to get filter string for the filter query for review aggregation
    :param body: payload of the API containing all the inputs
    :return: filter string
    """

    filter_cond = ''
    if body.get("brand"):
        brand_str = [x.lower() for x in body.get("brand")]
        brand_str = '","'.join(brand_str)
        filter_cond += f' and res_name in ("{brand_str}")'
        if body.get("city"):
            city_str = [x.lower() for x in body.get("city")]
            city_str = '","'.join(city_str)
            filter_cond += f' and city in ("{city_str}")'
            # if body.get("subZone"):
            #     filter_cond += f' and sub_zone = "{body.get("subZone").lower()}"'

    # if body.get("dish"):
    #     filter_cond = filter_cond + ' and item_name like "%' + body.get("dish").lower() + '%"'
    if body.get("comments"):
        filter_cond = filter_cond + ' and comments like "%' + body.get("comments").lower() + '%"'

    return filter_cond
