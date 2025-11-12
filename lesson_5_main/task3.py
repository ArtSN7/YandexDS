import pandas as pd
import numpy as np


def process(df):
    df['prediction'] = 1 - df['prediction']
    return df
