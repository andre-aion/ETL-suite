from scripts.storage.pythonClickhouse import PythonClickhouse
from config.df_construct_config import table_dict
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import load_cryptos

pc = PythonClickhouse('aion')
logger = mylogger(__file__)

def Table(table,table_alias,action,order_by):
    try:
        cols = sorted(list(table_dict[table_alias].keys()))
        if action == 'create':
            pc.create_table(table, table_dict[table_alias], cols,order_by)




    except Exception:
        logger.error("Table", exc_info=True)

