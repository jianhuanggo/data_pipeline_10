from typing import Union
import pandas as pd


def merge_csv(filepath1: str, filepath2: str, match_col: str, outfilepath: str):
    return pd.merge(pd.read_csv(filepath1), pd.read_csv(filepath2), on=match_col).to_csv(outfilepath)

