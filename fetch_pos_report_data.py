"""
This is the lambda function to make the auto reply to the zomato / google reviews
"""
import json
import sys
import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np


from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from utils import generate_traceback as api_traceback
from utils import generate_response as api_response
from utils import environment_config, constants
from utils.authentication import authenticate
from utils.constants import Const

from boto3 import client as boto3_client
import requests

lambda_client = boto3_client('lambda' , region_name='ap-south-1')

# Load variables from .env file
with open('env.env', 'r') as file:
    for line in file:
        key, value = line.strip().split('=')
        os.environ[key] = value
# logger client to enable logging
logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOG_LEVEL")))

env_object = environment_config.EnvironmentConfigurations()
try:
    api_secrets = env_object.get_value(os.environ.get("API_SECRETS"))
except ValueError as e:
    logger.error(str(e))
    sys.exit()

rds_host = api_secrets.get("rds_host")
user_name = api_secrets.get("rds_user_name")
password = api_secrets.get("rds_password")
db_name = api_secrets.get("rds_db_name")

try:
    conn = create_engine(f'mysql+pymysql://{user_name}:{password}@{rds_host}/{db_name}')
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to the MySQL instance.")
    sys.exit()


def fetch_data_from_db(query, conn, params=None):
    try:
        result = pd.read_sql(text(query), conn, params=params)
        return result
    except Exception as e:
        logger.error(f"Failed to retrieve data from the database: {str(e)}")
        return None
    

def fetch_data(query, conn):
    try:
        response_data = fetch_data_from_db(query, conn)
        return pd.DataFrame(response_data)
    except Exception as e:
        logger.error(f"Error executing SQL query: {str(e)}", extra={"query": query})
        return None
    
    

def fetch_pos_data():


        # Get the current date
    end_date = datetime.now()
    # Initialize an empty list to store data
    # all_data = []
    # for x in range(30):
    # # Calculate the current and one day back dates
    #     current_date = end_date - timedelta(days=x)
    #     one_day_back_date = current_date - timedelta(days=1)
    #     formatted_current_date = current_date.strftime('%Y-%m-%d')
    #     formatted_one_day_back_date = one_day_back_date.strftime('%Y-%m-%d')


        # Get today's date
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    # Calculate one month ago
    one_month_ago = today - timedelta(days=30)
    one_month_ago_yesterday = yesterday - timedelta(days=30)
    # Format the dates in the desired format (YYYY-MM-DD)
    today_formatted = today.strftime('%Y-%m-%d')
    yesterday_formatted = yesterday.strftime('%Y-%m-%d')

    one_month_ago_formatted = one_month_ago.strftime('%Y-%m-%d')
    one_month_ago_yesterday_formatted = one_month_ago_yesterday.strftime('%Y-%m-%d')

    print(f"today_formatted : {today_formatted}")
    print(f"one_month_ago_formatted : {one_month_ago_formatted}")
    print(f"yesterday_formatted :{yesterday_formatted}")
    print(f"one_month_ago_yesterday_formatted:{one_month_ago_yesterday_formatted} ")
    # breakpoint()
    query = f'''SELECT 
    current_day.invoice_day,
    current_day.Sale,
    current_day.Net_Sale,
    current_day.Orders,
    current_day.AOV,
    current_day.Net_AOV,
    current_day.Discount,
    current_day.total_cost,
    CONCAT(ROUND(((current_day.Orders - previous_day.Orders) / previous_day.Orders) * 100, 2), '%') AS Order_Growth,
    CONCAT(ROUND(((current_day.Sale - previous_day.Sale) / previous_day.Sale) * 100, 2), '%') AS Revenue_Growth,
    CONCAT(ROUND(((current_day.Net_Sale - previous_day.Net_Sale) / previous_day.Net_Sale) * 100, 2), '%') AS Net_Revenue_Growth,
    CONCAT(ROUND(((current_day.AOV - previous_day.AOV) / previous_day.AOV) * 100, 2), '%') AS AOV_Growth,
    CONCAT(ROUND(((current_day.Discount*-1) / current_day.Sale) * 100, 2), '%') AS Discount_Percentage
FROM 
    (
        -- Current Day Metrics Subquery
        SELECT 
            invoice_day,
            SUM(gross_amount) AS Sale,
            SUM(net_amount) AS Net_Sale,
            COUNT(*) AS Orders,
            SUM(gross_amount) / COUNT(*) AS AOV,
            SUM(net_amount) / COUNT(*) AS Net_AOV,
            SUM(JSON_EXTRACT(discounts, '$[0].amount')) AS Discount,
            SUM(total_cost) AS total_cost
        FROM 
            restaverse.rista_order_details
        WHERE 
            brand_name = "The Game Ranch"
            AND invoice_day BETWEEN "2024-02-07" AND "2024-03-07"
        GROUP BY invoice_day
    ) AS current_day
JOIN
    (
        -- Previous Day Metrics Subquery
        SELECT 
            invoice_day,
            SUM(gross_amount) AS Sale,
            SUM(net_amount) AS Net_Sale,
            COUNT(*) AS Orders,
            SUM(gross_amount) / COUNT(*) AS AOV,
            SUM(JSON_EXTRACT(discounts, '$[0].amount')) AS Discount
        FROM 
            restaverse.rista_order_details
        WHERE 
            brand_name = "The Game Ranch"
            AND invoice_day BETWEEN "2024-02-06" AND "2024-03-06"
        GROUP BY invoice_day
    ) AS previous_day ON current_day.invoice_day = DATE_ADD(previous_day.invoice_day, INTERVAL 1 DAY);
'''

    overallResults = fetch_data(query,conn)
    
    # Transpose the DataFrame
    transposed_overallResults = overallResults.T.reset_index()
    # Format the date column if it exists
    if 'invoice_day' in transposed_overallResults.columns:
        transposed_overallResults['invoice_day'] = pd.to_datetime(transposed_overallResults['invoice_day'])
    transposed_overallResults.insert(0, 'New Col','' )
    transposed_overallResults.iloc[0,0] = "Overall"
    # print(transposed_overallResults)
    # breakpoint()
    

# Individual Data frame i.e. zomato and swiggy

    query = '''SELECT 
    current_day.source_info_source,
    current_day.invoice_day,
    SUM(current_day.Sale) AS Sale,
    SUM(current_day.Net_Sale) AS Net_Sale,
    SUM(current_day.Orders) AS Orders,
    AVG(current_day.AOV) AS AOV,
    AVG(current_day.Net_AOV) AS Net_AOV,
    SUM(current_day.Discount) AS Discount,
    SUM(current_day.total_cost) AS total_cost,
    CONCAT(ROUND(((SUM(current_day.Orders) - COALESCE(SUM(previous_day.Orders), 0)) / COALESCE(SUM(previous_day.Orders), 1)) * 100, 2), '%') AS Order_Growth,
    CONCAT(ROUND(((SUM(current_day.Sale) - COALESCE(SUM(previous_day.Sale), 0)) / COALESCE(SUM(previous_day.Sale), 1)) * 100, 2), '%') AS Revenue_Growth,
    CONCAT(ROUND(((SUM(current_day.Net_Sale) - COALESCE(SUM(previous_day.Net_Sale), 0)) / COALESCE(SUM(previous_day.Net_Sale), 1)) * 100, 2), '%') AS Net_Revenue_Growth,
    CONCAT(ROUND(((AVG(current_day.AOV) - COALESCE(AVG(previous_day.AOV), 0)) / COALESCE(AVG(previous_day.AOV), 1)) * 100, 2), '%') AS AOV_Growth,
    CONCAT(ROUND(((SUM(current_day.Discount) / SUM(current_day.Sale)) * 100), 2), '%') AS Discount_Percentage
FROM 
    (
        -- Current Day Metrics Subquery
        SELECT 
            invoice_day,
            source_info_source,
            SUM(gross_amount) AS Sale,
            SUM(net_amount) AS Net_Sale,
            COUNT(*) AS Orders,
            SUM(gross_amount) / COUNT(*) AS AOV,
            SUM(net_amount) / COUNT(*) AS Net_AOV,
            SUM(JSON_EXTRACT(discounts, '$[0].amount')) AS Discount,
            SUM(total_cost) AS total_cost
        FROM 
            restaverse.rista_order_details
        WHERE 
            brand_name = "The Game Ranch"
            AND invoice_day BETWEEN "2024-02-07" AND "2024-03-07"
            AND source_info_source IN ('swiggy', 'zomato')  -- Add this line to filter sources
        GROUP BY source_info_source, invoice_day
    ) AS current_day
LEFT JOIN
    (
        -- Previous Day Metrics Subquery
        SELECT 
            invoice_day,
            source_info_source,
            SUM(gross_amount) AS Sale,
            SUM(net_amount) AS Net_Sale,
            COUNT(*) AS Orders,
            SUM(gross_amount) / COUNT(*) AS AOV,
            SUM(net_amount) / COUNT(*) AS Net_AOV,
            SUM(JSON_EXTRACT(discounts, '$[0].amount')) AS Discount
        FROM 
            restaverse.rista_order_details
        WHERE 
            brand_name = "The Game Ranch"
            AND invoice_day BETWEEN "2024-02-06" AND "2024-03-06"
            AND source_info_source IN ('swiggy', 'zomato')  -- Add this line to filter sources
        GROUP BY source_info_source, invoice_day
    ) AS previous_day ON current_day.invoice_day = DATE_ADD(previous_day.invoice_day, INTERVAL 1 DAY)
                     AND current_day.source_info_source = previous_day.source_info_source
GROUP BY current_day.source_info_source, current_day.invoice_day
ORDER BY current_day.source_info_source, current_day.invoice_day;'''

    results = fetch_data(query,conn)
    
    print(results)

    # Transpose the DataFrame
    transposed_results = results.T.reset_index()
    # Format the date column if it exists
    if 'invoice_day' in transposed_results.columns:
        transposed_results['invoice_day'] = pd.to_datetime(transposed_results['invoice_day'])



    # Split the DataFrame based on the 'Name' column
    contains_zomato = transposed_results.apply(lambda col: col.str.contains('zomato', case=False).iloc[0])
    contains_zomato_first = transposed_results.loc[:, contains_zomato]
    # does_not_contain_zomato = transposed_results.loc[:, ~contains_zomato]
    contains_no_zomato_second = transposed_results.loc[:, ~contains_zomato]

    first_column = contains_no_zomato_second.iloc[:, 0]
    contains_zomato_first.insert(0, 'Zomato', first_column)

    # print(contains_zomato_first)
    # print(contains_no_zomato_second)

        # Select the first 30 columns
    first_30_columns = transposed_results.iloc[:, :30]
    contains_zomato_first.insert(0, 'New_Column_Name', '')
    contains_zomato_first.iloc[1,0] = "Zomato"
    # Extract the first column of df1
    first_column = first_30_columns.iloc[:, 0]

    

    # Select the last 30 columns
    contains_no_zomato_second.insert(0, 'New_Column_Name', '')
    contains_no_zomato_second.iloc[1,0] = "Swiggy"
    print("hello")

    contains_zomato_first=contains_zomato_first.iloc[1:]
    contains_no_zomato_second=contains_no_zomato_second.iloc[1:]

    excel_filename = "trans_pos_report.xlsx"
    with pd.ExcelWriter(excel_filename, engine='xlsxwriter', date_format='yyyy-mm-dd') as writer:
        # transposed_results.to_excel(writer, sheet_name='Sheet1', index=False)
        transposed_overallResults.to_excel(writer, header=False,sheet_name='Sheet1', index=False)
        contains_zomato_first.to_excel(writer, header=False,sheet_name='Sheet1', index=False,startrow=contains_zomato_first.shape[0] + 1)
        contains_no_zomato_second.to_excel(writer, header=False, sheet_name='Sheet1', index=False, startrow=contains_zomato_first.shape[0]+contains_no_zomato_second.shape[0] + 2)
    
fetch_pos_data()
   