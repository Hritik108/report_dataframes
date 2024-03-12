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

s3 = boto3_client('s3')

lambda_client = boto3_client('lambda' , region_name='us-east-1')

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


#upload to s3 bucket
def upload_to_s3(local_file, bucket_name, file_key):
    # Create an S3 client

    try:
        # Upload the file
        s3.upload_file(local_file, bucket_name, file_key)
        # Set the object ACL to public-read
        s3.put_object_acl(Bucket=bucket_name, Key=file_key, ACL='public-read')
    
        print("Upload Successful")
        # Get the region of the S3 bucket
        bucket_region = s3.get_bucket_location(Bucket=bucket_name)['LocationConstraint'] or 'us-east-1'
        s3_url = f"https://{bucket_name}.s3.{bucket_region}.amazonaws.com/{file_key}"
        return s3_url
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


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

    #Get brands
    # query = f'''SELECT client_id FROM clients WHERE pos_report IS NOT NULL'''
    # client_ids = fetch_data(query,conn)
    # query = f'''SELECT res_id FROM restaurant_master_data WHERE client_id IN (SELECT client_id FROM clients WHERE pos_report IS NOT NULL)'''
    
#     query = '''SELECT rmd.res_id, rmd.brand, rmd.sub_zone
# FROM restaurant_master_data AS rmd JOIN rista_order_details_9_03_2024 AS rod ON rmd.res_id = rod.res_id
# WHERE rmd.client_id IN (SELECT client_id FROM clients WHERE pos_report IS NOT NULL)
# group by rmd.brand , rmd.sub_zone;'''
    
#     brands = fetch_data(query,conn)
    brands = ['Charcoal Eats','B burger']
    print(type(brands))
    print(brands)
    for brand in brands:
    # Access row values using row_values[index]
        print(brand)
        #Over all data query
        query = f'''SELECT 
        current_day.invoice_day,
        current_day.Sale,
        current_day.Net_Sale,
        current_day.Orders,
        current_day.AOV,
        current_day.Net_AOV,
        (current_day.Discount*-1) As Discount,
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
                rista_order_details_9_03_2024
            WHERE 
                brand_name = "{brand}"
                AND invoice_day BETWEEN "{one_month_ago_formatted}" AND "{today_formatted}"
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
                rista_order_details_9_03_2024
            WHERE 
                brand_name = "{brand}"
                AND invoice_day BETWEEN "{one_month_ago_yesterday_formatted}" AND "{yesterday_formatted}"
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
        

        # Individual Data query i.e. zomato and swiggy and rest depends on  source info source
        query = f'''SELECT 
        current_day.source_info_source,
        current_day.invoice_day,
        SUM(current_day.Sale) AS Sale,
        SUM(current_day.Net_Sale) AS Net_Sale,
        SUM(current_day.Orders) AS Orders,
        AVG(current_day.AOV) AS AOV,
        AVG(current_day.Net_AOV) AS Net_AOV,
        (SUM(current_day.Discount)) AS Discount,
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
                rista_order_details_9_03_2024
            WHERE 
                res_id = "{brand}"
                AND invoice_day BETWEEN "{one_month_ago_formatted}" AND "{today_formatted}"
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
                rista_order_details_9_03_2024
            WHERE 
                res_id = "{brand}"
                AND invoice_day BETWEEN "{one_month_ago_yesterday_formatted}" AND "{yesterday_formatted}"
                AND source_info_source IN ('swiggy', 'zomato')  -- Add this line to filter sources
            GROUP BY source_info_source, invoice_day
        ) AS previous_day ON current_day.invoice_day = DATE_ADD(previous_day.invoice_day, INTERVAL 1 DAY)
                        AND current_day.source_info_source = previous_day.source_info_source
    GROUP BY current_day.source_info_source, current_day.invoice_day
    ORDER BY current_day.source_info_source, current_day.invoice_day;'''
        
        query = f'''SELECT  
        current_day.source_info_source, 
        current_day.invoice_day, 
        SUM(current_day.Sale) AS Sale, 
        SUM(current_day.Net_Sale) AS Net_Sale, 
        SUM(current_day.Orders) AS Orders, 
        AVG(current_day.AOV) AS AOV, 
        AVG(current_day.Net_AOV) AS Net_AOV, 
        (SUM(current_day.Discount)*-1) AS Discount, 
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
                rista_order_details_9_03_2024 
            WHERE  
                brand_name = "{brand}"
                AND invoice_day BETWEEN  "{one_month_ago_formatted}"  AND "{today_formatted}"
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
                rista_order_details_9_03_2024    
            WHERE
                brand_name = "{brand}"
                AND invoice_day BETWEEN  "{one_month_ago_yesterday_formatted}" AND "{yesterday_formatted}"
            GROUP BY source_info_source, invoice_day 
        ) AS previous_day ON current_day.invoice_day = DATE_ADD(previous_day.invoice_day, INTERVAL 1 DAY) 
                        AND current_day.source_info_source = previous_day.source_info_source 
    GROUP BY current_day.source_info_source, current_day.invoice_day 
    ORDER BY current_day.source_info_source, current_day.invoice_day;'''

        results = fetch_data(query,conn)
        conn.dispose()
        print(results)
        distinct_values_list = results['source_info_source'].unique().tolist()
        # Define a custom sorting order
        sorting_order = ['zomato', 'swiggy']
        
        # Define a key function to sort based on the custom order
        key_function = lambda x: sorting_order.index(x) if x in sorting_order else len(sorting_order)

        # Sort the distinct values using the custom key function
        distinct_values_list = sorted(distinct_values_list, key=key_function)


        
        # Transpose the DataFrame
        transposed_results = results.T.reset_index()
        # Format the date column if it exists
        if 'invoice_day' in transposed_results.columns:
            transposed_results['invoice_day'] = pd.to_datetime(transposed_results['invoice_day'])
        print(results)
    
        length = 0
        excel_filename = "trans_pos_report.xlsx"

        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter', date_format='yyyy-mm-dd') as writer:
            transposed_overallResults.to_excel(writer, header=False,sheet_name='Sheet1', index=False,startrow=length)
            length = length + transposed_overallResults.shape[0]+1
            for source_info_source in distinct_values_list:
                filter_condition = transposed_results.apply(lambda col: col.str.contains(source_info_source, case=False).iloc[0])
                temp = transposed_results.loc[:, filter_condition]
                first_column = transposed_results.iloc[:, 0]
                temp.insert(0, source_info_source, first_column)
                temp.insert(0, 'New_Column_Name', '')
                temp.iloc[1,0] = source_info_source
                temp = temp.iloc[1:]
                print(temp)
                print(temp.shape[0])
                print(length)
                temp.to_excel(writer, header=False,sheet_name='Sheet1', index=False,startrow=length)
                length = length + temp.shape[0] + 1
        
        # local_file_path = "trans_pos_report.xlsx"
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

        
        # s3_url = upload_to_s3(excel_buffer, bucket_name, s3_file_key)



        client_email = 'hritik.chauhan@restaverse.com,vikhil@restaverse.com'
        subject = f"Monthly Pos Report from {today_formatted} to {one_month_ago_formatted}"
        draft = f"Monthly Pos Report from {today_formatted} to {one_month_ago_formatted} of {brand}"
        print(s3_url)
        file_name =f"{brand}_{one_month_ago_formatted}_to_{today_formatted}.xlsx"

    
        payload = {
                'pdf_s3_url' : s3_url,
                'subject':brand,
                'draft':draft,
                'type':'XLSX',
                'client_email':client_email,
                'from_date':one_month_ago_formatted,
                    'to_date':today_formatted,
                  'file_name' : file_name,
        }
        response = lambda_client.invoke(
                        FunctionName='arn:aws:lambda:us-east-1:908160969848:function:send_report_dev',
                        InvocationType='RequestResponse',
                        Payload=json.dumps({'body' :json.dumps(payload)})
                        )
       
            
        # print(response)


fetch_pos_data()
   

    #   payload = {
    #           'pdf_s3_url' : s3_url,
    #         'client_email':event['client_email'],
    #         'subject':subject,
    #         'draft':draft,
    #         'file_name' : file_name,
    #         'start_date':event['start_date'],
    #             'end_date':event['end_date'],
    #             'interval':event['interval'],
    #             'restaurantName':event['restaurantName']
    # }