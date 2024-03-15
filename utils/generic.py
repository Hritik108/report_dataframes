from typing import Hashable, List


# function to match two lists and get index of the matching values from second list
def match(a: List[Hashable], b: List[Hashable]) -> List[int]:
    b_dict = {x: i for i, x in enumerate(b)}
    return [b_dict.get(x, None) for x in a]