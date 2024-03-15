"""
A utility function to help with pagination params
"""
from utils.constants import ConfigConst
from utils.exceptions import InvalidFormatException


def paginate_start_end(req, default_order):
    if req is None:
        req = {}
    if 'order_by' in req.keys():
        sort_field = req['order_by']
    else:
        sort_field = default_order

    if 'skip' in req.keys():
        skip = req['skip']
    else:
        skip = ConfigConst.DEF_SKIP

    if 'top' in req.keys():
        top = req['top']
    else:
        top = ConfigConst.DEF_TOP

    if 'order' in req.keys():
        order = req['order']
    else:
        order = ConfigConst.DEF_ORDER

    if (int(skip) < 0) | (int(top) < 0):
        raise InvalidFormatException('Skip/top contain(s) negative value(s)')

    return ({
        'start': int(skip),
        'end': int(top),
        'sort_field': sort_field,
        'order': order
    })
