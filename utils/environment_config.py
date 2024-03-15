"""
Environment config would be used to fetch secret based key/value pairs using
AWS Parameter store. The Key/Value pairs need to be configured manually
"""
import os
import json
import logging

import boto3

from utils import constants
from utils import generate_traceback as api_traceback


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class EnvironmentConfigurations:
    """
    The class will use AWS Parameter store to store and retrieve secrets from AWS
    which can in turn then be used by any of the lambdas whenever required
    """
    def __init__(self):
        self.ssm_client = boto3.client("ssm", region_name=os.environ.get("REGION_NAME"))

    def get_value(self, name, version=None):
        """
        Gets the value of a parameter stored in Parameter store.

        Version (if defined) is used to retrieve a particular version of the secret.
        """
        kwargs = {"Name": name, "WithDecryption": True}
        if version is not None:
            kwargs["Name"] = f"{name}:version"
        try:
            response = self.ssm_client.get_parameter(**kwargs)

        except Exception:
            api_traceback.generate_system_traceback()
            raise ValueError(constants.Const.FAILED_TO_GET_ENV_CONFIG)
        else:
            response = json.loads(response["Parameter"]["Value"])
        return response
