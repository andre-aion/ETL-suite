import re
from scripts.utils.mylogger import mylogger


logger = mylogger(__file__)


import pandas as pd
import pdb
#import helper functions
from scripts.utils.myutils import optimize_dataframe
import gc

def get_first_N_of_string(x):
    return x[0:10]

def munge_blockdetails(df):
    # get first part of string
    df['addr'] = df.miner_address.map(lambda x: get_first_N_of_string(x), meta=('x',object))
    df['addr'] = df.addr.astype('category')
    df = df.drop(['miner_address'],axis=1)
    gc.collect()
    return df

def list_to_rows(df, column, sep=',', keep=False):
    """
    Split the values of a column and expand so the new DataFrame has one split
    value per row. Filters rows where the column is missing.

    Params
    ------
    df : pandas.DataFrame
        dataframe with the column to split and expand
    column : str
        the column to split and expand
    sep : str
        the string used to split the column's values
    keep : bool
        whether to retain the presplit value as it's own row

    Returns
    -------
    pandas.DataFrame
        Returns a dataframe with the same columns as `df`.
    """
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            # remove stray brackets
            value = re.sub('\[|\]',"",value)
            new_values.append(value)

    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    return new_df

# explode into new line for each list member
def explode_transaction_hashes(df):
    meta=('transaction_hashes',str)
    try:
        # remove quotes
        # explode the list
        df = list_to_rows(df,"transaction_hashes")

        return df
    except Exception:
        logger.error("explode transaction hashes", exc_info=True)

