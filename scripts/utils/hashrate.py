from pandas.api.types import is_string_dtype
from pdb import set_trace
import dask as dd
import gc
import config
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

def str_to_hex(x):
    return int(x, base=16)

def calc_hashrate(df,blockcount):
    df['rolling_mean'] = df.block_time.rolling(blockcount).mean()
    df = df.dropna()
    df['hashrate'] = df.difficulty / df.rolling_mean
    df = df.drop(['rolling_mean','difficulty'],axis=1)
    gc.collect()
    return df


def load_data(start, end):
    #find max block in dataset
    if start > end:
        start = end - 5000
        logger.warning("start > end, range adjusted")

    if start > config.max_block_loaded:
        start = config.max_block_loaded - 5000
        logger.warning("start block number is too high, "
                       "adjusted downwards to {}".format(start))
    if config.max_block_loaded < end:
        end = config.max_block_loaded
        logger.warning("starting block number is too high, "
                       "adjusted downwards to {}".format(start))

    if start < 0:
        start = 0
    logger.warning(config.block.get_df())
    df = config.block.get_df()[(int(config.block.get_df()['block_number']) >= start)
                               & (int(config.block.get_df()['block_number']) <= end)]

    return df

