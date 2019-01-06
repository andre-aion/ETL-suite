from scripts.utils.mylogger import mylogger

import logging
from clickhouse_driver import Client as Clickhouse_Client

import gc
from pdb import set_trace

logger = mylogger(__file__)

class PythonClickhouse:
    #port = '9000'

    def __init__(self):
        self.client = Clickhouse_Client('localhost')
        self.db = None

    def create_database(self, db='aion'):
        self.db = db
        sql = 'CREATE DATABASE IF NOT EXISTS {}'.format(self.db)
        self.client.execute(sql)

    # cols is a dict, key is colname, type is col type
    def construct_create_query(self,table,table_dict,columns):
        logger.warning('table_dict:%s', table_dict)

        count = 0
        try:
            qry = 'CREATE TABLE IF NOT EXISTS '+self.db+'.'+table+' ('
            if len(table_dict) >= 1:
                for key in columns:
                    if count > 0 :
                        qry += ','
                    if table == 'block':
                        logger.warning('%s:%s',key,table_dict[key])
                    qry += key+' '+table_dict[key]
                    count +=1
            qry +=") ENGINE = MergeTree() ORDER BY (block_timestamp)"

            logger.warning('create table query:%s', qry)
            return qry
        except Exception:
            logger.error("Construct table query")

    def create_table(self, table,table_dict,columns):
        try:
            qry = self.construct_create_query(table, table_dict, columns)
            self.client.execute(qry)
            return '{} SUCCESSFULLY CREATED (or exists)'.format(table.upper())
            logger.warning('%s SUCCESSFULLY CREATED', table)
        except Exception:
            return '{} NOT CREATED'.format(table.upper())
            logger.error("Create table error",exc_info=True)
    
    
    def construct_insert_query(self,table,cols,messages):
        # messages is list of tuples similar to cassandra
        qry = 'INSERT INTO '+self.db+'.'+self.table+' '+cols[table]+' VALUES '
        for idx, message in enumerate(messages):
            if idx > 0:
                qry +=','
            qry += message
        qry+= "'"
        logger.warning('data insert query:%s', qry)
        return qry
    
    def insert(self,table,cols,messages):
        qry = self.construct_insert_query(table,cols,messages)
        try:
            self.client.execute(qry)
            logger.warning('DATA SUCCESSFULLY INSERTED TO {}:%s', qry,table)
        except Exception:
            logger.error("Create table error",exc_info=True)


    def delete(self,item,type="table"):
        if type == 'table':
            self.client.execute("DROP TABLE IF EXISTS {}".format(item))
        logger.warning("%s deleted from clickhouse",item)