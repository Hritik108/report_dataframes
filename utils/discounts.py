"""
Common utility functions for discounts
"""


class SwiggyConstructs:
    constructs = {
        'overall': {"High Construct": '50-60%', "Medium Construct": '40%', "Low Construct": '30%'},
        'try_new': {"High Construct": '50-60%', "Medium Construct": '50%', "Low Construct": '40%'},
    }

    discounts = {
        '10%': 40, '20%': 50, '30%': 75, '40%': 80, '50%': 100, '60%': 120, '0%': 0
    }

    constants = {
        'base': 0.35,
        'high': 0.5
    }


class ZomatoConstructs:
    constructs = {
        'aggressive': {
            'la': {'High Construct': '50%-60%', 'Medium Construct': '40%', 'Low Construct': '20%', 'None': '0%'},
            'mm/um': {'High Construct': '40%', 'Medium Construct': '30%', 'Low Construct': '20%', 'None': '0%'}},
        'normal': {'la': {'High Construct': '40%', 'Medium Construct': '30%', 'Low Construct': '20%', 'None': '0%'},
                   'mm/um': {'High Construct': '30%', 'Medium Construct': '20%', 'Low Construct': '10%',
                             'None': '0%'}},
    }
    try_new_labels = {
        "Medium Construct": "High Construct", "Low Construct": "Medium Construct",
        "None": "Low Construct", "High Construct": "High Construct"
    }
    try_new_constructs = {
        'aggressive': {'High Construct': '60%', 'Medium Construct': '40%', 'Low Construct': '30%'},
        'normal': {'High Construct': '50%', 'Medium Construct': '30%', 'Low Construct': '20%'}
    }
    discounts = {
        '10%': 40, '20%': 50, '30%': 75, '40%': 80, '50%': 100, '60%': 120, '0%': 0
    }


class Constants:
    """
    Used for calculating prediction range
    """
    pred = 10
