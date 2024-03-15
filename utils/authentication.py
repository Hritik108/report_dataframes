"""This file has constant functions regarding the authentication of the user and the generation & update of access
tokens. """

import json
import logging
import jwt
import pandas as pd

from datetime import datetime, timedelta

from utils import exceptions
from utils.constants import AuthConst, ConfigConst
from utils import generate_traceback as api_traceback


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def _get_access_token(event, token_type):
    """
    Get the access token from the headers/body.
    :param event: input containing access/refresh token
    :param token_type: type of the token i.e. access/refresh token
    :return: jwt_token
    Return if token_type is access token return token from headers
           elif token_type is refresh token return from body
           or raise value error.
    """
    if token_type == 'access':
        try:
            jwt_token = event.get("headers").get("Authorization").split('Bearer ')[1]
        except Exception as e:
            logger.error(e)
            raise ValueError(AuthConst.MISSING_AUTH_TOKEN)
    elif token_type == 'refresh':
        try:
            jwt_token = json.loads(event.get("body")).get("refreshToken")
        except Exception as e:
            logger.error(e)
            raise ValueError(AuthConst.MISSING_AUTH_TOKEN)
    else:
        raise ValueError(AuthConst.MISSING_AUTH_TOKEN)
    return jwt_token


def authenticate(event, token_type, jwt_secret, conn):
    """
    Method to authenticate a given jwt_token by decoding the token using given jwt_secret
    :param event: input containing access/refresh token
    :param token_type: string, access/refresh
    :param jwt_secret: string, jwt secret corresponding to the token type
    :param conn: database connection instance
    :return:
        1. client_id: client_id of the user authenticated by token
        2. email: email of the user encoded in jwt token
    """
    jwt_token = _get_access_token(event, token_type)
    try:
        decoded_token = jwt.decode(jwt_token, jwt_secret, algorithms=['HS256'])
    except jwt.ExpiredSignatureError as e:
        logger.error(e)
        raise jwt.ExpiredSignatureError(AuthConst.EXPIRED_TOKEN)
    except (jwt.InvalidSignatureError, jwt.DecodeError) as e:
        logger.error(e)
        raise ValueError(AuthConst.INVALID_TOKEN)

    query = f"select {token_type}_token from users where client_id = {decoded_token['clientId']} and " \
            f"status = 'active' and lower(email) = '{decoded_token['emailId'].lower()}' and " \
            f"status in ('active', 'invited') "
    records = pd.read_sql(query, conn)

    if len(records) == 0:
        raise ValueError(AuthConst.USER_DOES_NOT_EXIST)
    elif records.iloc[0, 0] != jwt_token:
        raise ValueError(AuthConst.INVALID_TOKEN)

    return decoded_token['clientId'], decoded_token['emailId'].lower()


def create_jwt_tokens(client_id, email, access_token_secret, refresh_token_secret):
    """
    :param email: email of the new user
    :param client_id: client_id for which we have to insert the new user
    :param access_token_secret: secret for creating access token
    :param refresh_token_secret: secret for creating refresh token
    :return: access_token, access_token_expiry, refresh_token, refresh_token_expiry
    """
    access_token_expiry = datetime.utcnow() + timedelta(days=ConfigConst.ACCESS_TOKEN_EXPIRY)
    item = {
        "clientId": client_id,
        "emailId": email,
        "exp": access_token_expiry,
    }
    access_token = jwt.encode(item, access_token_secret, algorithm="HS256")

    refresh_token_expiry = datetime.utcnow() + timedelta(days=ConfigConst.REFRESH_TOKEN_EXPIRY)
    item.update({"exp": refresh_token_expiry})
    refresh_token = jwt.encode(item, refresh_token_secret, algorithm="HS256")
    return access_token, access_token_expiry, refresh_token, refresh_token_expiry


def update_jwt_tokens(conn, email, client_id, access_token_secret, refresh_token_secret):
    """
    :param conn: database connection instance
    :param email: email of the new user
    :param client_id: client_id for which we have to insert the new user
    :param access_token_secret: secret for creating access token
    :param refresh_token_secret: secret for creating refresh token
    :return: access_token, refresh_token
    """
    # create JWT tokens
    access_token, access_token_expiry, refresh_token, refresh_token_expiry = \
        create_jwt_tokens(client_id, email, access_token_secret, refresh_token_secret)
    # access_token = access_token.decode("utf-8")
    # refresh_token = refresh_token.decode("utf-8")

    sql = f"UPDATE users SET modified_on_utc = '{datetime.utcnow()}'," \
          f" access_token = '{access_token}', access_token_expiry = '{access_token_expiry}'," \
          f" refresh_token = '{refresh_token}', refresh_token_expiry = '{refresh_token_expiry}' " \
          f" WHERE email = '{email}' and client_id = {client_id}"

    try:
        conn.execute(sql)
    except:
        api_traceback.generate_system_traceback()
        raise exceptions.DBExecutionException
    return access_token, refresh_token
