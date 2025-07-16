from fuzzysearch import find_near_matches

def fuzzysearch(sequence: str, query: str, max_dist: int) -> list:
    return find_near_matches(query, sequence, max_l_dist=max_dist)