"""
This file contains util functions for getting client details
"""

import pandas as pd

from utils import exceptions


def get_client_details(conn, client_id):
    """
    :param conn: database connection
    :param client_id: client id
    :return: dict of client details
    """

    query = f"select * from clients where client_id = {client_id}"
    try:
        result = pd.read_sql(query, conn, parse_dates=['expiry_date'])
    except:
        raise exceptions.DBExecutionException
    result = result.to_dict('records')[0]
    return result
