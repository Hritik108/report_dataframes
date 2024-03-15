import pandas as pd
from urllib.parse import unquote


def read_xlsx_from_s3(s3, bucket, filename):
    """
    :param s3: S3 client
    :param bucket: S3 bucket where file is stored
    :param filename: S3 key of file
    :return: excel df
    """
    filename = unquote(filename.replace('+', ' '))
    obj = s3.get_object(Bucket=bucket, Key=filename)
    xlsx_df = pd.ExcelFile(obj['Body'].read())
    return xlsx_df


def read_csv_from_s3(s3, bucket, filename):
    """
    :param s3: S3 client
    :param bucket: S3 bucket where file is stored
    :param filename: S3 key of file
    :return: excel df
    """
    filename = unquote(filename.replace('+', ' '))
    obj = s3.get_object(Bucket=bucket, Key=filename)
    csv_df = pd.read_csv(obj['Body'])
    return csv_df
