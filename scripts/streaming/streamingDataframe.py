import pandas as pd
import dask as dd
from dask.dataframe import from_pandas,from_array
import gc
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

# Manage dask dataframes from streaming library
class StreamingDataframe:
    def __init__(self, table_name, columns, dedup_columns):
        try:
            self.partitions = 15
            self.table_name = table_name
            logger.warning("init:%s",table_name)
            logger.warning("init:%s",columns)

            self.columns = columns[table_name]
            self.dedup_columns = dedup_columns[table_name]
            # initialize as pandas
            df = pd.DataFrame(columns=columns)
            # convert to dask
            self.df = from_pandas(df, npartitions=self.partitions,
                                  name=table_name, sort=True)
        except Exception:
            logger.error("init:%s", exc_info=True)


    # data: dict of lists from rdds
    def add_data(self, data, chunksize=500):
        # create temp dataframe from dict
        df = pd.DataFrame.from_dict(data)
        df_temp = from_pandas(df, npartitions=self.partitions,sort=True)
        # append to current array
        try:
            self.df = self.df.append(df_temp)
            self.deduplicate()

        except Exception :
            logger.error("add data", exc_info=True)
        del df
        del data
        gc.collect()


    # drop duplicates by values in columns
    def deduplicate(self):
        try:
            self.df.drop_duplicates(subset=self.dedup_columns, keep='last', inplace=True)
        except Exception:
            logger.error("DEDUPLICATON ERROR WITH", exc_info=True)


    # truncate data frame
    # position : top or bottom
    def truncate_data(self,numrows, row_indices_list, column='block_timestamp'):
        self.df.drop(row_indices_list, inplace=True)

    def get_df(self):
        return self.df

