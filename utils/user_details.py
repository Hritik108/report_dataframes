import pandas as pd


def get_user_details(conn, client_id, email):
    """
    :param conn: database connection
    :param client_id: client id of the user
    :param email: email of the user
    :return: dict of user details
    """

    query = f"select * from users where client_id = {client_id} and email = '{email}' and status = 'active'"
    try:
        result = pd.read_sql(query, conn)
    except:
        raise

    result = result.to_dict('records')
    return result
