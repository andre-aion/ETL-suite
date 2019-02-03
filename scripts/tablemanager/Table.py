from scripts.storage.pythonClickhouse import PythonClickhouse
from config.df_construct_config import table_dict
from scripts.utils.mylogger import mylogger

pc = PythonClickhouse('aion')
logger = mylogger(__file__)

def Table(table,table_alias,action):
    try:
        cols = sorted(list(table_dict[table_alias].keys()))
        if action == 'create':
            text = pc.create_table(table,table_dict[table_alias],cols)


    except Exception:
        logger.error("Table", exc_info=True)

