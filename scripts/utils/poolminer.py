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

#def get_miner_list(df):
