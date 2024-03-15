"""
Utility functions used commonly across facebook review lambdas
"""
import requests
import pandas as pd
from utils.constants import Const


def api_request(url, logger, method="get"):
    """
    A utility function to make api requests to facebook
    @:params:
        url: url of the api call
        logger: logger connection
    @:returns
        list of either len = 1 or len = 2 as a validation if error has occurred
    """
    try:
        if method == "post":
            request = requests.post(url=url)
        else:
            request = requests.get(url=url)
        data = request.json()
        if data.get('error', None) is not None:
            message = {"message": data['error']['message']}
            logger.warning(f"{data['error']['message']}")
            if request.status_code == 200:
                status_code = 401
            else:
                status_code = 500
            # status_code = 500
            return [status_code, message]
        else:
            return [data]
    except:
        message = {"message": Const.EXT_API_FAILURE}
        logger.warning(f"{Const.EXT_API_FAILURE}")
        status_code = 500
        return [status_code, message]


def read_access_token_data(access_token_query, conn, logger):
    """
    A utility function to read access token for facebook pages
    @:params:
        access_token_query: string query to be executed in sql
        conn: database connection
        logger: logger connection
    @:returns
        list of either len = 1 or len = 2 as a validation if error has occurred
    """
    try:
        access_token_data = pd.read_sql(access_token_query, conn)
    except:
        message = {"message": Const.DB_FAILURE}
        logger.warning(f"{Const.DB_FAILURE}")
        return [500, message]

    if len(access_token_data.index) == 0:
        logger.error(f"{Const.AUTH_TOKEN_NOT_GENERATED}")
        message = {"message": Const.AUTH_TOKEN_NOT_GENERATED}
        return [500, message]

    return [access_token_data]
