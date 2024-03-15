"""
This file contains utility functions to access google apis
"""
import json

import pandas as pd
import time

import requests

from utils.constants import Const
from utils.exceptions import DBExecutionException, RecordNotFoundException, GoogleAPIException


def get_access_token(conn, api_secrets):
    """
    This function returns the access token or refresh and generates new token
    """

    query = f"select name from google_review_accounts where client_id = 3"

    try:
        result = pd.read_sql(query, conn)
    except:
        raise DBExecutionException

    data = json.loads(result["name"][0])
    access_token = data.get("accessToken")
    refresh_token = data.get("refreshToken")
    expiry = data.get("expiry")

    if expiry > time.time():
        return access_token
    else:
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Bearer {access_token}'
        }
        payload = f'client_id={api_secrets.get("google_auth_client_id")}&client_secret=' \
                  f'{api_secrets.get("google_auth_client_secret")}&grant_type=refresh_token' \
                  f'&refresh_token={refresh_token}'

        url = "https://oauth2.googleapis.com/token"

        try:
            response = requests.request("POST", url, headers=headers, data=payload)
        except:
            raise GoogleAPIException

        if response.status_code != 200:
            raise GoogleAPIException

        response_body = response.json()
        access_token = response_body.get("access_token")
        refresh_token = refresh_token
        expiry = time.time() + 3500

        query = f"update google_review_accounts set name = '{json.dumps({'accessToken': access_token, 'refreshToken': refresh_token, 'expiry': expiry})}' where client_id = 3"
        try:
            conn.execute(query)
        except:
            raise DBExecutionException

        return access_token
