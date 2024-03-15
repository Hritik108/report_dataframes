"""
A utility function to generate response that would be sent as API response
"""
import json
from typing import Dict
import numpy as np


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def generate_response(status_code: int, response_body: Dict or str, location: str = None, cookies: Dict = {}):
    """
    Create a proper response which will be sent back as response to the API
    :param status_code: integer (specifies 200, 400, 500, etc.)
    :param response_body: dict (message, variables that need to be sent as part of response)
    :param location: str (if a redirection needs to be done we should get location as a param)
    :param cookies: dict (if a cookies needs to be set we should get cookies as a param)
    :return: dict (the actual response that would be sent back to the API)
    """
    response_dict = {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(response_body, cls=NpEncoder),
    }
    if location:
        response_dict.get("headers").update({"Location": location})

    if cookies:
        response_dict['multiValueHeaders'] = cookies

    return response_dict
