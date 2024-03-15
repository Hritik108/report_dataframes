"""
A utility function to generate system traceback whenever some exception might occur
"""
import sys
import json
import logging
import traceback

# logger client to enable logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def generate_system_traceback():
    """
    A simple function which will log the sys traceback in case any exception occurs
    :param: None
    :return: None
    """
    exception_type, exception_value, exception_traceback = sys.exc_info()
    traceback_string = traceback.format_exception(
        exception_type, exception_value, exception_traceback
    )
    err_msg = json.dumps(
        {
            "errorType": exception_type.__name__,
            "errorMessage": str(exception_value),
            "stackTrace": traceback_string,
        }
    )
    logger.error(err_msg)
