import sys
from contextlib import closing

from scripts.utils.mylogger import mylogger
import luigi
from datetime import datetime
from luigi.contrib.mysqldb import MySqlTarget

from clickhouse_driver import Client as Clickhouse_Client
from airflow import DAG, operators
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine

import gc
from pdb import set_trace

logger = mylogger(__file__)
DEFAULT_DATE = datetime(2018, 4, 23)

class Luigi:
    mysql_host = "40.113.226.240"
    mysql_password = "1233tka061",
    mysql_username = "kafka"
    mysql_port = "3306"
    mysql_db = "aion"

    mysql_hostname = "127.0.0.1"
    mysql_password = "password"
    mysql_username = "admin1"
    mysql_db = 'aionv4'

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2018, 4, 1),
        'email': ['andremayers@yahoo.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
    }
    def __init__(self, cols, table):
        self.cols = cols
        self.table = table
        dag = DAG('aion_analytics', default_args=self.default_args,
                  schedule_interval=timedelta(minutes=1))

    def get_uri(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        login = ''
        if conn.login:
            login = '{conn.login}:{conn.password}@'.format(conn=conn)
        host = conn.host
        if conn.port is not None:
            host += ':{port}'.format(port=conn.port)
        return '{conn.conn_type}://{login}{host}/{conn.schema}'.format(
            conn=conn, login=login, host=host)

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        if engine_kwargs is None:
            engine_kwargs = {}

        return create_engine(self.get_uri(), **engine_kwargs)


    def get_conn(self):
        """Returns a connection object
        """
        return self.connector.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            username=self.mysql_username,
            password=self.mysql_password,
            schema=self.mysql_db)

    def construct_read_query(self):
        count = 0
        try:
            qry = 'SELECT '
            if len(self.columns) >= 1:
                for key in self.columns:
                    if count > 0:
                        qry += ','
                    qry += key
                    count += 1
            else:
                qry += '*'
            qry += ' FROM '+self.mysql_db+'.'+self.table

            logger.warning('create table query:%s', qry)
            return qry
        except Exception:
            logger.error("Construct table query")

    def get_records(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records.
        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')

        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)

        return cur.fetchall()


    def mysql_operator(self):
        sql =  self.construct_read_query()

        import airflow.operators.mysql_operator
        t = operators.mysql_operator.MySqlOperator(
            task_id='basic_mysql',
            sql=sql,
            mysql_conn_id='airflow_db',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.


        """
        schema = map(lambda schema_tuple: schema_tuple[0], cursor.description)
        file_no = 0
        for row in cursor:
            print(row)