from bokeh.layouts import gridplot
from bokeh.models import Div, Panel

from scripts.storage.pythonClickhouse import PythonClickhouse
from config.df_construct_config import table_dict
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import tab_error_flag

pc = PythonClickhouse('aion')
logger = mylogger(__file__)

def Table(table,table_alias,action):
    try:
        cols = list(table_dict[table_alias].keys())
        if action == 'create':
            text = pc.create_table(table,table_dict[table_alias],cols)

        div = Div(text=text)

        # create the dashboard
        grid = gridplot([[div]])

        # Make a tab with the layout
        tab = Panel(child=grid, title='Blockminer')
        return tab

    except Exception:
        logger.error("Graph draw", exc_info=True)

        return tab_error_flag('block')

