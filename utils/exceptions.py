# pylint: disable=useless-super-delegation
"""
This module contains the custom exception classes which are used in this application.
"""
from utils.constants import Const


class RestaverseException(Exception):
    """
    This exception should be raised when the format of certain strings are not valid like email.
    """

    def __init__(self, message=None, status_code=None) -> None:
        """
        Class arguments message & status_code are optional.
        """
        if message is None:
            self.message = Const.INTERNAL_SERVER_ERROR
        else:
            self.message = message
        if status_code is None:
            self.status_code = 500
        else:
            self.status_code = status_code

        super().__init__(self.message, self.status_code)

    def __str__(self) -> str:
        return super().__str__()


class SecretNotFoundException(RestaverseException):
    """
    This exception should be raised if the program is unable to find secret key.
    """

    def __init__(self, message=Const.INTERNAL_SERVER_ERROR, status_code=500) -> None:
        super(SecretNotFoundException, self).__init__(message, status_code)


#
# class HeaderOrTokenMissingException(RestaverseException):
#     """
#     This exception should be raised when access token is missing.
#     """
#
#     def __init__(self, message=AuthConst.MISSING_AUTH_TOKEN, status_code=404) -> None:
#         super(HeaderOrTokenMissingException, self).__init__(message, status_code)


# class ExpiredTokenException(RestaverseException):
#     """
#     This exception should be raised when JWT tokens are expired.
#     """
#
#     def __init__(self, message=AuthConst.EXPIRED_TOKEN, status_code=401) -> None:
#         super(ExpiredTokenException, self).__init__(message, status_code)
#
#
# class InvalidTokenException(RestaverseException):
#     """
#     This exception should be raise when token is invalid.
#     """
#
#     def __init__(self, message=AuthConst.INVALID_TOKEN, status_code=400) -> None:
#         super(InvalidTokenException, self).__init__(message, status_code)


class RecordNotFoundException(RestaverseException):
    """
    This exception should be raised when DB table does not have the requested item.
    """

    def __init__(self, message=Const.MISSING_RECORD, status_code=404) -> None:
        super(RecordNotFoundException, self).__init__(message, status_code)


class FailedToSendEmailException(RestaverseException):
    """
    This exception should be raised when the format of certain strings are not valid like email.
    """

    def __init__(self, message=Const.INTERNAL_SERVER_ERROR, status_code=500) -> None:
        super(FailedToSendEmailException, self).__init__(message, status_code)


class InvalidFormatException(RestaverseException):
    """
    This exception should be raised when the format of certain strings are not valid like email.
    """

    def __init__(self, message=Const.INVALID_FORMAT, status_code=400) -> None:
        super(InvalidFormatException, self).__init__(message, status_code)


class DBExecutionException(RestaverseException):
    """
    This exception should be raised when there are exceptions while running any queries on database
    """

    def __init__(self, message=Const.INTERNAL_SERVER_ERROR, status_code=500) -> None:
        super(DBExecutionException, self).__init__(message, status_code)


class ColumnMissingException(RestaverseException):
    """
    This exception should be raised when there are missing columns in dataframe
    """

    def __init__(self, message=Const.MISSING_COLUMN_ERROR, status_code=400) -> None:
        super(ColumnMissingException, self).__init__(message, status_code)


class InvalidAggregationLevel(RestaverseException):
    """
    This exception should be raised when the aggregation level values is invalid
    """

    def __init__(self, message=Const.INVALID_AGGREGATION_LEVEL, status_code=400) -> None:
        super(InvalidAggregationLevel, self).__init__(message, status_code)


class TabMissingException(RestaverseException):
    """
    This exception should be raised when there are mandatory tabs missing in zomato/swiggy excel
    """

    def __init__(self, message=Const.MISSING_TAB_ERROR, status_code=400) -> None:
        super(TabMissingException, self).__init__(message, status_code)


class MultipleTabsInReviewFile(RestaverseException):
    """
    This exception should be raised when review file contains more than one tab
    """

    def __init__(self, message=Const.MULTIPLE_TABS_IN_REVIEW_FILE, status_code=400) -> None:
        super(MultipleTabsInReviewFile, self).__init__(message, status_code)


class InvalidVisualizationKpi(RestaverseException):
    """
    This exception should be raised when KPI being request for visualization is invalid
    """

    def __init__(self, message=Const.INVALID_VISUALIZATION_KPI, status_code=400) -> None:
        super(InvalidVisualizationKpi, self).__init__(message, status_code)


class BenchMarkingNotDefined(RestaverseException):
    """
    This exception when benchmarking is not defined for restaurant health
    """

    def __init__(self, message=Const.BENCHMARKING_NOT_DEFINED, status_code=500) -> None:
        super(BenchMarkingNotDefined, self).__init__(message, status_code)


class MasterDataNotDefined(RestaverseException):
    """
    This exception when master data is not defined for some specific client
    """

    def __init__(self, message=Const.MASTER_DATA_NOT_DEFINED, status_code=500) -> None:
        super(MasterDataNotDefined, self).__init__(message, status_code)


class GoogleAPIException(RestaverseException):
    """
    This exception when google api fails to fetch data
    """

    def __init__(self, message=Const.GOOGLE_API_FAILURE, status_code=500) -> None:
        super(GoogleAPIException, self).__init__(message, status_code)

# class NonCorporateEmailException(RestaverseException):
#     """
#     This exception should be raised if the non corporate emails are provided.
#     """
#
#     def __init__(self, message=Const.non_corp_emails, status_code=400) -> None:
#         super(NonCorporateEmailException, self).__init__(message, status_code)


# class ExistingRecordsException(RestaverseException):
#     """
#     This exception should be raised when db table must not have duplicate items.
#     """
#
#     def __init__(self, message=Const.in_use, status_code=400) -> None:
#         super(ExistingRecordsException, self).__init__(message, status_code)
