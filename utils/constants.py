"""
This file contains constants like configuration values, error message and success messages
for api responses and to be used across the project across all the APIs
"""


class ConfigConst:
    """
    This class will contain the list of all the config params
    """
    DEF_SKIP = 0
    DEF_TOP = 50
    DEF_ORDER = "DESC"
    DOWNLOAD_LINK_EXPIRY = 120  # in seconds
    UPLOAD_LINK_EXPIRY = 120  # in seconds
    ACCESS_TOKEN_EXPIRY = 14  # in days
    REFRESH_TOKEN_EXPIRY = 365  # in days
    DEF_FB_LIMIT = 10
    PLATFORMS = ['swiggy', 'zomato']
    MODEL_ENGINE = "gpt-3.5-turbo-0613"
    GPT_PROMPT_INGRED = "Can you tell me 3 different food descriptions with maximum 30 words for {}? {} contains {}."
    GPT_PROMPT = "Can you tell me 3 different food descriptions with maximum 30 words for {}?"
    GPT_PROMPT_END = ' Give the output in JSON in the following format: {"option1": "..","option2": "..","option3": "..",}'


class AuthConst:
    """
    This class defines the messages as constants for Auth.
    """

    INVALID_TOKEN = "Invalid token"
    MISSING_AUTH_TOKEN = "Unable to find token in headers"
    UNABLE_TO_DECODE = "Unable to decode provided authentication token"
    EXPIRED_TOKEN = "Token has expired"
    USER_DOES_NOT_EXIST = "No matching user for the given authentication token"
    NO_AUTH_TYPE = "No auth type was provided"
    AUTH_CONFIG_NOT_SET = "Auth configuration not set"


class Const:
    """
    This class will contain all the API responses both success and error messages
    """
    GOOGLE_API_FAILURE = "Failure while calling the Google API"
    MISSING_SUBZONE = "SubZone not provided in inputs"
    GPT_FAILURE = "Failure while calling the GPT API"
    RESTAURANT_ALREADY_EXIST = "Restaurant already exists"
    INVALID_DATE = "Invalid date"
    MULTIPLE_TABS_IN_REVIEW_FILE = "Aggregator review file should have only one tab"
    INVALID_AGGREGATION_LEVEL = "Value for aggregation level should be on of the Monthly/Weekly/Daily"
    AGGREGATION_LEVEL_NOT_DEFINED = "Value for aggregation level not defined for this client"
    BENCHMARKING_NOT_DEFINED = "Appropriate benchmarking is not defined in restaurant health"
    MASTER_DATA_NOT_DEFINED = "Master data is not defined for the client"
    MISSING_COLUMN_ERROR = "Columns missing in input data"
    MISSING_TAB_ERROR = "Tab missing in input data"
    INVALID_VISUALIZATION_KPI = "Visualization KPI being requested for is invalid"
    INTERNAL_SERVER_ERROR = "Internal Server Error"
    MISSING_RECORD = "No record found in db"
    INVALID_FORMAT = "Input not in the expected format"
    MISSING_TRANSACTION_ID = "Transaction id not provided in inputs"
    MISSING_QUERY_PARAMS = "Query parameters not provided in inputs"
    SUCCESS = "Success"
    FAILED_TO_GET_ENV_CONFIG = "Unable to fetch environment configuration"
    DB_FAILURE = "Failure while querying database"
    S3_FAILURE = "Failure while accessing S3"
    AUTH_TOKEN_NOT_GENERATED = "Access Token not generated for the given credential"
    ISSUE_GENR_AUTH = "Some issue with generating the auth URL(s)"
    EXT_API_FAILURE = "Failure while calling the external API"
    MISSING_PARA = "Parameters not provided in inputs"
    RECORD_PRESENT = "Record is already present"
    EXISTING_USER = "Cannot modify existing user"
    UNAUTH_USER = "User does not have permission"
    MISSING_PLATFORM = {"platform": ["required field"]},
    INSUFFICIENT_DATA = "Insufficient data to process the request",
    PLAT_SUBZONE = "Platform or SubZone Mandatory"
    EXPIRED_CLIENT = "Please contact your admin to renew their subscription if your brand is already on restaverse."
    MISSING_DATA = "Data for {} is considered. {} Data is missing. Please upload {} Data to get overall insights"

class Ads:
    """
    This class defines the constants for Ads which make a range
    """
    BASE = 0.15
    HIGH = 0.07

    CTO_LOW_BASE = 0.05
    CTO_LOW_HIGH = 0.15
    CTO_HIGH_BASE = 0.05
    CTO_HIGH_HIGH = 0.1
    CTO_MEDIUM_BASE = 0.07
    CTO_MEDIUM_HIGH = 0.07
