"""
This is the lambda function to make the auto reply to the zomato / google reviews
"""
import json
import sys
import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from utils import environment_config
from boto3 import client as boto3_client
from botocore.exceptions import NoCredentialsError
from io import BytesIO
import numpy as np

s3 = boto3_client('s3')

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

def check_null_dates(transposed_results):
    if 'invoice_day' in transposed_results.columns:
        # Convert 'invoice_day' column to datetime
        transposed_results['invoice_day'] = pd.to_datetime(transposed_results['invoice_day'])

        # Set 'invoice_day' as the index
        transposed_results.set_index('invoice_day', inplace=True)

        # Create a date range from the minimum to maximum date in the index
        date_range = pd.date_range(start=transposed_results.index.min(), end=transposed_results.index.max(), freq='D')

        # Reindex the DataFrame with the date range
        transposed_results = transposed_results.reindex(date_range)

        # Reset the index to bring 'invoice_day' back as a column
        transposed_results.reset_index(inplace=True)

        # Rename the index column back to 'invoice_day'
        transposed_results.rename(columns={'index': 'invoice_day'}, inplace=True)

        # Convert 'invoice_day' column back to string (if needed)
        transposed_results['invoice_day'] = transposed_results['invoice_day'].dt.strftime('%Y-%m-%d')

        # Fill missing values in other columns with null
        transposed_results.fillna(value=pd.NA, inplace=True)

        # Reset index
        transposed_results.reset_index(drop=True, inplace=True)

        # Remove any potential duplicate rows
        transposed_results.drop_duplicates(inplace=True)

        # Drop the 'index' column (if it exists)
        transposed_results.drop(columns=['index'], errors='ignore', inplace=True)

        # Print or do further processing if needed
        print(transposed_results)

        return transposed_results


def fetch_pos_data():

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

    #brands = fetch_data(query,conn)
    brands = ['Charcoal Eats','B burger']
    for brand in brands:
    # Access row values using row_values[index]

        #Over all data query
        query = f'''SELECT
                calendar.invoice_day,
                COALESCE(current_day.Sale, 0) AS Sale,
                COALESCE(current_day.Net_Sale, 0) AS Net_Sale,
                COALESCE(current_day.Orders, 0) AS Orders,
                COALESCE(current_day.AOV, 0) AS AOV,
                COALESCE(current_day.Net_AOV, 0) AS Net_AOV,
                COALESCE((current_day.Discount * -1), 0) As Discount,
                COALESCE(current_day.total_cost, 0) AS total_cost,
                CONCAT(ROUND(((current_day.Orders - previous_day.Orders) / NULLIF(previous_day.Orders, 0)) * 100, 2), '%') AS Order_Growth,
                CONCAT(ROUND(((current_day.Sale - previous_day.Sale) / NULLIF(previous_day.Sale, 0)) * 100, 2), '%') AS Revenue_Growth,
                CONCAT(ROUND(((current_day.Net_Sale - previous_day.Net_Sale) / NULLIF(previous_day.Net_Sale, 0)) * 100, 2), '%') AS Net_Revenue_Growth,
                CONCAT(ROUND(((current_day.AOV - previous_day.AOV) / NULLIF(previous_day.AOV, 0)) * 100, 2), '%') AS AOV_Growth,
                CONCAT(ROUND(((current_day.Discount * -1) / NULLIF(current_day.Sale, 0)) * 100, 2), '%') AS Discount_Percentage
            FROM
                (
                    SELECT DISTINCT
                        DATE_ADD("{one_month_ago_yesterday_formatted}", INTERVAL seq DAY) AS invoice_day
                    FROM
                        (
                            SELECT
                                ROW_NUMBER() OVER() - 1 AS seq
                            FROM
                                information_schema.tables
                        ) AS seq
                    WHERE
                        DATE_ADD("{one_month_ago_formatted}", INTERVAL seq DAY) <= "{today_formatted}"
                ) AS calendar
            LEFT JOIN
                (
                    -- Current Day Metrics Subquery
                    SELECT
                        DATE(invoice_day) AS invoice_day,
                        SUM(gross_amount) AS Sale,
                        SUM(net_amount) AS Net_Sale,
                        COUNT(*) AS Orders,
                        SUM(gross_amount) / COUNT(*) AS AOV,
                        SUM(net_amount) / COUNT(*) AS Net_AOV,
                        SUM(JSON_EXTRACT(discounts, '$[0].amount')) AS Discount,
                        SUM(total_cost) AS total_cost
                    FROM
                        rista_order_details_9_03_2024
                    WHERE
                        brand_name = "{brand}"
                        AND invoice_day BETWEEN "{one_month_ago_formatted}" AND "{today_formatted}"
                    GROUP BY
                        DATE(invoice_day)
                ) AS current_day ON calendar.invoice_day = current_day.invoice_day
            LEFT JOIN
                (
                    -- Previous Day Metrics Subquery
                    SELECT
                        DATE(invoice_day) AS invoice_day,
                        SUM(gross_amount) AS Sale,
                        SUM(net_amount) AS Net_Sale,
                        COUNT(*) AS Orders,
                        SUM(gross_amount) / COUNT(*) AS AOV,
                        SUM(JSON_EXTRACT(discounts, '$[0].amount')) AS Discount
                    FROM
                        rista_order_details_9_03_2024
                    WHERE
                        brand_name = "{brand}"
                        AND invoice_day BETWEEN "{one_month_ago_yesterday_formatted}" AND "{yesterday_formatted}"
                    GROUP BY
                        DATE(invoice_day)
                ) AS previous_day ON calendar.invoice_day = DATE_ADD(previous_day.invoice_day, INTERVAL 1 DAY);

                    '''

        overallResults = fetch_data(query,conn)

        # Transpose the DataFrame
        transposed_overallResults = overallResults.T.reset_index()
        transposed_overallResults.fillna(0, inplace=True)
        # Format the date column if it exists
        if 'invoice_day' in transposed_overallResults.columns:
            transposed_overallResults['invoice_day'] = pd.to_datetime(transposed_overallResults['invoice_day'])
        transposed_overallResults.insert(0, 'New Col','' )
        transposed_overallResults.iloc[0,0] = "Overall"

        # Individual Data query i.e. zomato and swiggy etc and rest depends on  source info sourc

        query = f'''
                    SELECT
                source.source_info_source AS source_info_source,
                calendar.invoice_day,
                COALESCE(SUM(current_day.Sale), 0) AS Sale,
                COALESCE(SUM(current_day.Net_Sale), 0) AS Net_Sale,
                COALESCE(SUM(current_day.Orders), 0) AS Orders,
                AVG(current_day.AOV) AS AOV,
                AVG(current_day.Net_AOV) AS Net_AOV,
                COALESCE(SUM(current_day.Discount) * -1, 0) AS Discount,
                COALESCE(SUM(current_day.total_cost), 0) AS total_cost,
                CONCAT(
                    ROUND(
                        (
                            (
                                SUM(current_day.Orders) - COALESCE(SUM(previous_day.Orders), 0)
                            ) / COALESCE(SUM(previous_day.Orders), 1)
                        ) * 100,
                        2
                    ),
                    '%'
                ) AS Order_Growth,
                CONCAT(
                    ROUND(
                        (
                            (
                                SUM(current_day.Sale) - COALESCE(SUM(previous_day.Sale), 0)
                            ) / COALESCE(SUM(previous_day.Sale), 1)
                        ) * 100,
                        2
                    ),
                    '%'
                ) AS Revenue_Growth,
                CONCAT(
                    ROUND(
                        (
                            (
                                SUM(current_day.Net_Sale) - COALESCE(SUM(previous_day.Net_Sale), 0)
                            ) / COALESCE(SUM(previous_day.Net_Sale), 1)
                        ) * 100,
                        2
                    ),
                    '%'
                ) AS Net_Revenue_Growth,
                CONCAT(
                    ROUND(
                        (
                            (
                                AVG(current_day.AOV) - COALESCE(AVG(previous_day.AOV), 0)
                            ) / COALESCE(AVG(previous_day.AOV), 1)
                        ) * 100,
                        2
                    ),
                    '%'
                ) AS AOV_Growth,
                CONCAT(
                    ROUND(
                        (
                            (
                                SUM(current_day.Discount) / SUM(current_day.Sale)
                            ) * 100
                        ),
                        2
                    ),
                    '%'
                ) AS Discount_Percentage
            FROM
                (
                    SELECT DISTINCT
                        DATE_ADD("{one_month_ago_yesterday_formatted}", INTERVAL seq DAY) AS invoice_day
                    FROM
                        (
                            SELECT
                                ROW_NUMBER() OVER() - 1 AS seq
                            FROM
                                information_schema.tables
                        ) AS seq
                    WHERE
                        DATE_ADD("{one_month_ago_formatted}", INTERVAL seq DAY) <= "{today_formatted}"
                ) AS calendar
            CROSS JOIN
                (
                    SELECT DISTINCT
                        source_info_source
                    FROM
                        rista_order_details_9_03_2024
                    WHERE
                        brand_name = "{brand}"
                ) AS source
            LEFT JOIN
                (
                    -- Current Day Metrics Subquery
                    SELECT
                        DATE(invoice_day) AS invoice_day,
                        source_info_source,
                        SUM(gross_amount) AS Sale,
                        SUM(net_amount) AS Net_Sale,
                        COUNT(*) AS Orders,
                        SUM(gross_amount) / COUNT(*) AS AOV,
                        SUM(net_amount) / COUNT(*) AS Net_AOV,
                        SUM(JSON_EXTRACT(discounts, '$[0].amount')) AS Discount,
                        SUM(total_cost) AS total_cost
                    FROM
                        rista_order_details_9_03_2024
                    WHERE
                        brand_name = "{brand}"
                        AND invoice_day BETWEEN "{one_month_ago_formatted}" AND "{today_formatted}"
                    GROUP BY
                        DATE(invoice_day),
                        source_info_source
                ) AS current_day ON calendar.invoice_day = current_day.invoice_day AND source.source_info_source = current_day.source_info_source
            LEFT JOIN
                (
                    -- Previous Day Metrics Subquery
                    SELECT
                        DATE(invoice_day) AS invoice_day,
                        source_info_source,
                        SUM(gross_amount) AS Sale,
                        SUM(net_amount) AS Net_Sale,
                        COUNT(*) AS Orders,
                        SUM(gross_amount) / COUNT(*) AS AOV,
                        SUM(net_amount) / COUNT(*) AS Net_AOV,
                        SUM(JSON_EXTRACT(discounts, '$[0].amount')) AS Discount
                    FROM
                        rista_order_details_9_03_2024
                    WHERE
                        brand_name = "{brand}"
                        AND invoice_day BETWEEN "{one_month_ago_yesterday_formatted}" AND "{yesterday_formatted}"
                    GROUP BY
                        DATE(invoice_day),
                        source_info_source
                ) AS previous_day ON calendar.invoice_day = DATE_ADD(previous_day.invoice_day, INTERVAL 1 DAY) AND COALESCE(current_day.source_info_source, 'magicpin') = COALESCE(previous_day.source_info_source, 'magicpin')
            GROUP BY
                calendar.invoice_day,
                source.source_info_source
            ORDER BY
                calendar.invoice_day,
                source.source_info_source;
            '''

        results = fetch_data(query,conn)
        conn.dispose()

        distinct_values_list = results['source_info_source'].unique().tolist()
        # Define a custom sorting order
        sorting_order = ['zomato', 'swiggy']

        # Define a key function to sort based on the custom order
        key_function = lambda x: sorting_order.index(x) if x in sorting_order else len(sorting_order)

        # Sort the distinct values using the custom key function
        distinct_values_list = sorted(distinct_values_list, key=key_function)


        # Transpose the DataFrame
        transposed_results = results.T.reset_index()

        # #call a function and insert the null data from empty data
        # transposed_results = check_null_dates(transposed_results)
        # breakpoint()

        # Format the date column if it exists
        if 'invoice_day' in transposed_results.columns:
            transposed_results['invoice_day'] = pd.to_datetime(transposed_results['invoice_day'])

        length = 0
        excel_filename = "trans_pos_report.xlsx"

        transposed_results.fillna(0, inplace=True)

        excel_buffer = BytesIO()


        # Define the Excel writer
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter', date_format='yyyy-mm-dd') as writer:
            transposed_overallResults.to_excel(writer, header=False, sheet_name='Sheet1', index=False, startrow=length)
            length += transposed_overallResults.shape[0] + 1

            # Access the workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']

            # Set all columns to center alignment
            for col_num, value in enumerate(transposed_overallResults.columns.values):
                worksheet.set_column(col_num, col_num, None, None, {'align': 'center'})

            for source_info_source in distinct_values_list:
                filter_condition = transposed_results.apply(lambda col: col.str.contains(source_info_source, case=False).iloc[0])
                temp = transposed_results.loc[:, filter_condition]

                first_column = transposed_results.iloc[:, 0]
                temp.insert(0, source_info_source, first_column)
                temp.insert(0, 'New_Column_Name', '')
                temp.iloc[1, 0] = source_info_source
                temp = temp.iloc[1:]

                temp.to_excel(writer, header=False, sheet_name='Sheet1', index=False, startrow=length)
                length += temp.shape[0] + 1

            # Set the uniform column width
            uniform_width = 20  # Adjust the width as needed
            for col in range(temp.shape[1]):
                worksheet.set_column(col, col, uniform_width)

        local_file_path = "trans_pos_report.xlsx"
        bucket_name = "email-cron"
        s3_file_key = f"pos_report/{brand}_{one_month_ago_formatted}_to_{today_formatted}.xlsx"

        # Reset the position of the buffer to the beginning
        excel_buffer.seek(0)
        try:
            s3.upload_fileobj(excel_buffer, bucket_name, s3_file_key)
            s3.put_object_acl(Bucket=bucket_name, Key=s3_file_key, ACL='public-read')
            bucket_region = s3.get_bucket_location(Bucket=bucket_name)['LocationConstraint'] or 'us-east-1'
            s3_url = f"https://{bucket_name}.s3.{bucket_region}.amazonaws.com/{s3_file_key}"
            print("S3 URL:", s3_url)

        except FileNotFoundError:
            print("The file was not found")

        client_email = 'hritik.chauhan@restaverse.com,vikhil@restaverse.com'
        subject = f"Monthly Pos Report from {today_formatted} to {one_month_ago_formatted}"
        draft = f"Monthly Pos Report from {today_formatted} to {one_month_ago_formatted} of {brand}"
        print(s3_url)
        file_name =f"{brand}_{one_month_ago_formatted}_to_{today_formatted}.xlsx"

        # payload = {
        #         'pdf_s3_url' : s3_url,
        #         'subject':brand,
        #         'draft':draft,
        #         'type':'XLSX',
        #         'client_email':client_email,
        #         'from_date':one_month_ago_formatted,
        #         'to_date':today_formatted,
        #         'file_name' : file_name,
        # }
        # response = lambda_client.invoke(
        #                 FunctionName='send_mail_from_client',
        #                 InvocationType='RequestResponse',
        #                 Payload=json.dumps({'body' :json.dumps(payload)})
        #                 )
fetch_pos_data()
