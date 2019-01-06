# GLOBAL VARIABLES
columns = {}
insert_sql = {}
dedup_cols = {}
create_table_sql = {}
create_indexes= {}
table_dict = {}
columns_ch={}
columns_ch_all ={}
table_dict_all={}
columns['block'] = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "block_size", "block_timestamp","block_date",
               "block_year", "block_month",
               "block_day", "num_transactions",
               "block_time", "approx_nrg_reward", "transaction_hashes"]

columns_ch['block'] = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "block_size", "block_timestamp","block_date", "year", "month",
               "day", "num_transactions",
               "block_time", "approx_nrg_reward", "transaction_hashes"]

columns_ch_all['block'] = ["block_number", "block_hash","miner_address", "parent_hash",
               "receipt_tx_root", "state_root","tx_trie_root","extra_data",
               "nonce", "bloom","solution","difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "block_size", "block_timestamp", "num_transactions",
               "block_time", "approx_nrg_reward","transaction_hash", "transaction_hashes",
               "year", "month","day"]



dedup_cols['block'] = ['block_number']
insert_sql['block'] = """
                    INSERT INTO block_old(block_number, miner_address, 
                    miner_addr, nonce, difficulty, 
                    total_difficulty, nrg_consumed, nrg_limit,
                    block_size, block_timestamp, block_date, block_year, 
                    block_month, block_day, num_transactions,
                    block_time, approx_nrg_reward, transaction_hashes) 
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """


create_table_sql['block'] = """
                CREATE TABLE IF NOT EXISTS block (block_number bigint,
                                              miner_address varchar, miner_addr varchar,
                                              nonce varchar, difficulty bigint, 
                                              total_difficulty bigint, nrg_consumed bigint, nrg_limit bigint,
                                              block_size bigint, block_timestamp timestamp, block_date timestamp, 
                                              block_year smallint, block_month tinyint, block_day tinyint,
                                              num_transactions smallint, block_time int, approx_nrg_reward float, 
                                              transaction_hashes varchar,
                                              PRIMARY KEY (block_number));
                 """

table_dict_all['block'] = {
                        'block_number':'UInt64',
                        'block_hash':'String',
                        'miner_address' : 'String',
                        'parent_hash' : 'String',
                        'receipt_tx_root':'String',
                        'state_root':'String',
                        'tx_trie_root':'String',
                        'extra_data':'String',
                        'nonce' : 'String',
                        'bloom':'String',
                        'solution':'String',
                        'difficulty' : 'UInt64',
                        'total_difficulty' : 'UInt64',
                        'nrg_consumed' : 'UInt64',
                        'nrg_limit' : 'UInt64',
                        'block_size' : 'UInt64',
                        'block_timestamp' : 'Datetime',
                        'num_transactions': 'UInt16',
                        'block_time' : 'UInt64',
                        'nrg_reward':'Float64',
                        'approx_nrg_reward': 'Float64',
                        'transaction_hash':'String',
                        'transaction_hashes': 'String',
                        'year': 'UInt16',
                        'month': 'UInt8',
                        'day': 'UInt8'

}

table_dict['block'] = {

}


create_indexes['block'] = [
        "CREATE INDEX IF NOT EXISTS block_block_year_idx ON block (block_year);",
        "CREATE INDEX IF NOT EXISTS block_block_month_idx ON block (block_month);",
        "CREATE INDEX IF NOT EXISTS block_block_day_idx ON block (block_day);",
        "CREATE INDEX IF NOT EXISTS block_block_date_idx ON block (block_date);",
        "CREATE INDEX IF NOT EXISTS block_miner_address_idx ON block (miner_address);"
    ]

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             TRANSACTIONS
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dedup_cols['transaction'] = ['transaction_hash']
columns_ch['transaction'] = ['transaction_hash','transaction_index','block_number',
                       'transaction_timestamp','block_timestamp',"block_date",
                       'from_addr','to_addr','approx_value','nrg_consumed',
                       'nrg_price','nonce','contract_addr','year',
                       'month','day']

columns['transaction'] = ['transaction_hash','transaction_index','block_number',
                       'transaction_timestamp','block_timestamp',"block_date",
                       'from_addr','to_addr','approx_value','nrg_consumed',
                       'nrg_price','nonce','contract_addr','transaction_year',
                       'transaction_month','transaction_day']

insert_sql['transaction'] = """ INSERT INTO transaction_old(
            transaction_hash,transaction_index, block_number,
            transaction_timestamp,block_timestamp, block_date, 
            from_addr, to_addr, approx_value, 
            nrg_consumed, nrg_price, nonce, contract_addr,
            transaction_year, transaction_month, transaction_day)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

create_table_sql['transaction'] = """
                CREATE TABLE IF NOT EXISTS transaction (
                      transaction_hash varchar,transaction_index smallint, block_number bigint,
                      transaction_timestamp bigint,block_timestamp timestamp, block_date timestamp,
                      from_addr varchar, to_addr varchar, approx_value float,
                      nrg_consumed bigint, nrg_price bigint, nonce varchar, contract_addr varchar,
                      transaction_year smallint, transaction_month tinyint, transaction_day tinyint,
                      PRIMARY KEY (block_number, transaction_index)
                      ) WITH CLUSTERING ORDER BY (transaction_index ASC);
                """

create_indexes['transaction'] = [
        "CREATE INDEX IF NOT EXISTS transaction_transaction_year_idx ON transaction (transaction_year);",
        "CREATE INDEX IF NOT EXISTS transaction_transaction_month_idx ON transaction (transaction_month);",
        "CREATE INDEX IF NOT EXISTS transaction_transaction_day_idx ON transaction (transaction_day);",
        "CREATE INDEX IF NOT EXISTS transaction_block_date_idx ON transaction (block_date);",
        "CREATE INDEX IF NOT EXISTS transaction_transaction_timestamp_idx ON transaction (transaction_timestamp);",
        "CREATE INDEX IF NOT EXISTS transaction_from_addr_idx ON transaction (from_addr);",
        "CREATE INDEX IF NOT EXISTS transaction_to_addr_idx ON transaction (to_addr);",
        "CREATE INDEX IF NOT EXISTS transaction_contract_addr_idx ON transaction (contract_addr);"
]

table_dict['transaction'] = {
                                'transaction_hash' : 'String',
                                'transaction_index' : 'UInt16',
                                'block_number' : 'UInt64',
                                'transaction_timestamp' : 'UInt64',
                                'block_timestamp' : 'Datetime',
                                'block_date' : 'Date',
                                'from_addr' : 'String',
                                'to_addr' : 'String',
                                'approx_value': 'Float64',
                                'nrg_consumed': 'UInt64',
                                'nrg_price': 'UInt64',
                                'nonce': 'String',
                                'contract_addr' : 'String',
                                'transaction_year' : 'UInt16',
                                'transaction_month' : 'UInt8',
                                'transaction_day' :  'UInt8'
                            }

table_dict_all['transaction'] = {
    'transaction_hash' : 'String',
    'block_hash':'String',
    'block_number' : 'UInt64',
    'block_timestamp': 'Datetime',
    'transaction_index': 'UInt16',
    'from_addr' : 'String',
    'to_addr' : 'String',
    'nrg_consumed': 'UInt64',
    'nrg_price': 'UInt64',
    'transaction_timestamp': 'UInt64',
    'value': 'Float64',
    'approx_value': 'Float64',
    'transaction_log' : 'String',
    'data': 'String',
    'nonce': 'String',
    'tx_error': 'String',
    'contract_addr' : 'String',
    'year' : 'UInt16',
    'month' : 'UInt8',
    'day' :  'UInt8'
}