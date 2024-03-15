"""
Probable set of re-usable validations on various types for use cases
"""
import re


def validate_email_regex(email):
    """
    Function to validate email regex
    :param email: str
    :return: true/False
    """
    if re.match(r"^[\w\.-]+@[\w\.-]+(\.[\w]+)+", email, flags=re.IGNORECASE):
        return True
    return False
