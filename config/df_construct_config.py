# GLOBAL VARIABLES
columns = {}
insert_sql = {}
dedup_cols = {}
create_table_sql = {}
create_indexes= {}
table_dict = {}
table_dict_all = {}
columns_ch = {}
columns_ch_all ={}

load_columns = {
    'block':dict(),
    'transaction':dict(),
    'block_tx_warehouse':dict(),
}

warehouse_inputs = {'block_tx_warehouse':dict()}

load_columns['block']['churn'] = ['transaction_hashes', 'block_timestamp', 'miner_address',
                  'block_number','difficulty','nrg_consumed','nrg_limit',
                  'block_size','block_time','approx_nrg_reward']



columns_ch_all['block'] = ["block_number", "miner_address", "miner_addr",
               "nonce", "difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "block_size", "block_timestamp","block_date", "year", "month",
               "day", "num_transactions",
               "block_time", "approx_nrg_reward", "transaction_hashes"]

columns['block'] = ["block_number", "block_hash","miner_address", "parent_hash",
               "receipt_tx_root", "state_root","tx_trie_root","extra_data",
               "nonce", "bloom","solution","difficulty",
               "total_difficulty", "nrg_consumed", "nrg_limit",
               "block_size", "block_timestamp", "num_transactions",
               "block_time", "approx_nrg_reward","transaction_hash", "transaction_hashes",
               "year", "month","day"]

dedup_cols['block'] = ['block_number']

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             TRANSACTIONS
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dedup_cols['transaction'] = ['transaction_hash']
columns['transaction'] = ['transaction_hash','block_hash','block_number','block_timestamp',
                          'transaction_index', 'from_addr','to_addr','nrg_consumed',
                            'approx_value','nrg_price','transaction_timestamp',
                       'value','transaction_log','data','nonce','tx_error',
                      'contract_addr','year','month','day']

load_columns['transaction']['churn'] = ['block_timestamp',
                        'transaction_hash', 'from_addr',
                        'to_addr', 'approx_value', 'nrg_consumed']


table_dict['transaction'] = {
    'transaction_hash' : 'String',
    'block_hash':'String',
    'block_number' : 'UInt64',
    'block_timestamp': 'Datetime',
    'transaction_index': 'UInt16',
    'from_addr' : 'String',
    'to_addr' : 'String',
    'nrg_consumed': 'UInt64',
    'approx_value': 'Float64',
    'nrg_price': 'UInt64',
    'transaction_timestamp': 'UInt64',
    'value': 'Float64',
    'transaction_log' : 'String',
    'data': 'String',
    'nonce': 'String',
    'tx_error': 'String',
    'contract_addr' : 'String',
    'year' : 'UInt16',
    'month' : 'UInt8',
    'day' :  'UInt8'
}

columns_ch['transaction'] = ['transaction_hash','transaction_index','block_number',
                       'transaction_timestamp','block_timestamp',"block_date",
                       'from_addr','to_addr','approx_value','nrg_consumed',
                       'nrg_price','nonce','contract_addr','year',
                       'month','day']

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             block tx warehouse poolminer
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load_columns['block_tx_warehouse']['churn'] = ['block_timestamp', 'block_number', 'to_addr',
                      'from_addr', 'miner_address', 'approx_value','transaction_hash',
                      'block_nrg_consumed','transaction_nrg_consumed','difficulty',
                      'nrg_limit','block_size','block_time', 'approx_nrg_reward']

columns['block_tx_warehouse'] = ['block_number','block_timestamp','transaction_hash','miner_address',
                'total_difficulty','difficulty',
                'block_nrg_consumed','nrg_limit','num_transactions',
                'block_size','block_time','approx_nrg_reward','block_year','block_month',
                'block_day','from_addr',
                'to_addr', 'approx_value', 'transaction_nrg_consumed','nrg_price']

dedup_cols['block_tx_warehouse'] = []

table_dict['block_tx_warehouse'] = {
    'miner_address':'String',
    'block_number' : 'UInt64',
    'block_timestamp' : 'Datetime',
    'block_size' : 'UInt64',
    'block_time': 'UInt64',
    'difficulty': 'Float64',
    'total_difficulty':'Float64',
    'nrg_limit':'UInt64',
    'transaction_hash': 'String',
    'approx_nrg_reward':'Float64',
    'from_addr': 'String',
    'to_addr': 'String',
    'num_transactions': 'UInt16',
    'block_nrg_consumed':'UInt64',
    'transaction_nrg_consumed': 'UInt64',
    'nrg_price': 'UInt64',
    'approx_value': 'Float64',
    'block_year': 'UInt16',
    'block_month': 'UInt8',
    'block_day':  'UInt8'

}

warehouse_inputs['block_tx_warehouse']['block'] = ['transaction_hashes','miner_address',
                'block_number','block_timestamp','total_difficulty','difficulty',
                'nrg_consumed','nrg_limit','num_transactions',
                'block_size','block_time','approx_nrg_reward','year','month','day']


warehouse_inputs['block_tx_warehouse']['transaction'] = [
                'transaction_hash', 'from_addr',
                'to_addr', 'approx_value', 'nrg_consumed','nrg_price']



# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#                             block tx warehouse poolminer
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
table_dict['checkpoint'] = {
    'table':'String',
    'column':'String',
    'offset':'String',
    'timestamp':'Datetime'
}

table_dict['block'] = {
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

columns['checkpoint'] = ['table','column','offset','timestamp']

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# WHEN JOINED, WHEN CHURNED
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
table_dict['miner_activity'] = {
    'block_timestamp': 'Date',
    'tier1_new_miners': 'String',
    'tier1_churned_miners':'String',
    'tier1_retained_miners':'String',
    'tier1_active_miners':'String',
    'tier1_new': 'UInt64',
    'tier1_churned':'UInt64',
    'tier1_retained':'UInt64',
    'tier1_active':'UInt64',
    'tier2_new_miners': 'String',
    'tier2_churned_miners':'String',
    'tier2_retained_miners':'String',
    'tier2_active_miners':'String',
    'tier2_new': 'UInt64',
    'tier2_churned':'UInt64',
    'tier2_retained':'UInt64',
    'tier2_active':'UInt64',
    'block_size': 'Float64',
    'block_time': 'Float64',
    'difficulty': 'Float64',
    'nrg_limit': 'Float64',
    'approx_nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'block_nrg_consumed': 'Float64',
    'transaction_nrg_consumed': 'Float64',
    'nrg_price': 'Float64',
    'approx_value': 'Float64',
    'block_year': 'UInt16',
    'block_month': 'UInt16',
    'block_day':'UInt16',
    'day_of_week':'String'
}

columns['miner_activity'] = [
    'block_timestamp',
    'tier1_new_miners','tier1_churned_miners','tier1_retained_miners','tier1_active_miners',
    'tier1_new', 'tier1_churned', 'tier1_retained','tier1_active',
    'tier2_new_miners','tier2_churned_miners','tier2_retained_miners', 'tier2_active_miners',
    'tier2_new', 'tier2_churned', 'tier2_retained','tier2_active',
    'block_size', 'block_time','difficulty', 'nrg_limit',
    'approx_nrg_reward' , 'num_transactions','block_nrg_consumed','nrg_price',
    'approx_value', 'transaction_nrg_consumed',
    'block_year','block_month', 'block_day', 'day_of_week']


################################################################
#            MODEL FUNCTION
#################################################################
'''
def model(self, date):
    try:
        pass
    except Exception:
        logger.error('', exc_info=True)

'''