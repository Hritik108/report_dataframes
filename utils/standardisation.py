import re
from datetime import datetime


def standardize_col_names(df, column_mapping, tab_name, suffix):
    """
    standardize column names of zomato and swiggy dataframe
    :param df: swiggy/zomato dataframe for which column names are to be standardized
    :param column_mapping: reference column mapping for standardization
    :param tab_name: tab_name for customization related to ads
    :param suffix: suffix for adding to column names for respective sheet
    :return: dataframe with standardized column names
    """
    # mapping column names
    df.columns = df.columns.str.lower().str.replace('%', '').str.strip().str.replace(' ', '_')

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
    df.columns = df.columns.str.lower().str.replace('%', '').str.strip().str.replace(' ', '_')
    df.columns = eval(f'[n + "{suffix}" for n in df.columns]')
    return df


def to_date(s):
    """
    Function to convert string to date
    :param s: date
    :return: datetime
    """
    return datetime.strptime(s, '%d-%m-%Y')