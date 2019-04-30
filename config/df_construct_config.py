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
                            'p','nrg_price','transaction_timestamp',
                       'value','transaction_log','data','nonce','tx_error',
                      'contract_addr','year','month','day']

load_columns['transaction']['churn'] = ['block_timestamp',
                        'transaction_hash', 'from_addr',
                        'to_addr', 'p', 'nrg_consumed']


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
                      'from_addr', 'miner_address', 'p','transaction_hash',
                      'block_nrg_consumed','transaction_nrg_consumed','difficulty',
                      'nrg_limit','block_size','block_time', 'nrg_reward']

columns['block_tx_warehouse'] = ['block_number','block_timestamp','transaction_hash','miner_address',
                'total_difficulty','difficulty',
                'block_nrg_consumed','nrg_limit','num_transactions',
                'block_size','block_time','nrg_reward','block_year','block_month',
                'block_day','from_addr',
                'to_addr', 'value','transaction_nrg_consumed','nrg_price']

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
    'nrg_reward':'Float64',
    'from_addr': 'String',
    'to_addr': 'String',
    'num_transactions': 'UInt16',
    'block_nrg_consumed':'UInt64',
    'transaction_nrg_consumed': 'UInt64',
    'nrg_price': 'UInt64',
    'value': 'Float64',
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
table_dict['network_activity'] = {
    'block_timestamp': 'Date',
    'from_addr_new_lst': 'String',
    'from_addr_churned_lst':'String',
    'from_addr_retained_lst':'String',
    'from_addr_active_lst':'String',
    'from_addr_new': 'UInt64',
    'from_addr_churned':'UInt64',
    'from_addr_retained':'UInt64',
    'from_addr_active':'UInt64',
    'to_addr_new_lst': 'String',
    'to_addr_churned_lst':'String',
    'to_addr_retained_lst':'String',
    'to_addr_active_lst':'String',
    'to_addr_new': 'UInt64',
    'to_addr_churned':'UInt64',
    'to_addr_retained':'UInt64',
    'to_addr_active':'UInt64',
    'block_size': 'Float64',
    'block_time': 'Float64',
    'difficulty': 'Float64',
    'nrg_limit': 'Float64',
    'approx_nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'block_nrg_consumed': 'Float64',
    'transaction_nrg_consumed': 'Float64',
    'nrg_price': 'Float64',
    'value': 'Float64',
    'block_year': 'UInt16',
    'block_month': 'UInt16',
    'block_day':'UInt16',
    'day_of_week':'String'
}

columns['network_activity'] = [
    'block_timestamp',
    'from_addr_new_lst', 'from_addr_churned_lst', 'from_addr_retained_lst', 'from_addr_active_lst',
    'from_addr_new','from_addr_churned','from_addr_retained','from_addr_active',
    'to_addr_new_lst','to_addr_churned_lst','to_addr_retained_lst', 'to_addr_active_lst',
    'to_addr_new', 'to_addr_churned', 'to_addr_retained', 'to_addr_active',
    'block_size', 'block_time','difficulty', 'nrg_limit',
    'nrg_reward' , 'num_transactions','block_nrg_consumed','nrg_price',
    'value', 'transaction_nrg_consumed',
    'block_year','block_month', 'block_day', 'day_of_week']

################################################################
#            AION account
#################################################################
table_dict['account_balance'] = {
    'address': 'String',
    'block_day': 'UInt8',
    'block_month': 'UInt8',
    'block_number': 'UInt64',
    'block_timestamp': 'Datetime',
    'block_year': 'UInt16',
    'day_of_week': 'String',
    'from_addr':'String',
    'to_addr': 'String',
    'transaction_hash':'String',
    'value': 'Float64'

}

table_dict['account_activity'] = {
    'activity':'String',
    'address': 'String',
    'block_day': 'UInt8',
    'block_hour': 'UInt8',
    'block_month': 'UInt8',
    'block_number': 'UInt64',
    'block_timestamp': 'Datetime',
    'block_year': 'UInt16',
    'day_of_week': 'String',
    'event':'String',
    'account_type':'String',
    'from_addr':'String',
    'to_addr': 'String',
    'transaction_hash':'String',
    'value': 'Float64',

}

table_dict['account_activity_churn'] = {
    'block_timestamp': 'Date',
    'new_lst': 'String',
    'churned_lst':'String',
    'retained_lst':'String',
    'active_lst':'String',
    'new': 'UInt64',
    'churned':'UInt64',
    'retained':'UInt64',
    'active':'UInt64',
    'value': 'Float64',
    'value_counts':'UInt64',
    'block_size': 'Float64',
    'block_time': 'Float64',
    'difficulty': 'Float64',
    'nrg_limit': 'Float64',
    'nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'block_nrg_consumed': 'Float64',
    'transaction_nrg_consumed': 'Float64',
    'nrg_price': 'Float64',
    'block_year': 'UInt16',
    'block_month': 'UInt16',
    'block_day':'UInt16',
    'day_of_week':'String',

}

table_dict['account_value_churn'] = {
    'block_timestamp': 'Datetime',
    'address':'String',
    'value': 'Float64',
    'transaction_hash':'String',
    'block_size': 'Float64',
    'block_time': 'Float64',
    'difficulty': 'Float64',
    'nrg_limit': 'Float64',
    'nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'block_nrg_consumed': 'Float64',
    'transaction_nrg_consumed': 'Float64',
    'nrg_price': 'Float64',
    'block_year': 'UInt16',
    'block_month': 'UInt16',
    'block_day':'UInt16',
    'day_of_week':'String',
    'activity':'String'

}

table_dict['account_activity_warehouse'] = {
    'block_timestamp': 'Datetime',
    'activity': 'String',
    'address':'String',
    'event':'String',
    'account_type':'String',
    'amount': 'Float64',
    'block_size': 'Float64',
    'block_time': 'Float64',
    'difficulty': 'Float64',
    'nrg_limit': 'Float64',
    'nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'block_nrg_consumed': 'Float64',
    'transaction_nrg_consumed': 'Float64',
    'nrg_price': 'Float64',
    'block_year': 'UInt16',
    'block_month': 'UInt16',
    'block_day':'UInt16',
    'day_of_week':'String',
    'from_addr':'String',
    'to_addr':'String',
    'transaction_hash':'String',
    'sp_volume':'Float64',
    'sp_close':'Float64',
    'russell_volume': 'Float64',
    'russell_close': 'Float64',
    'aion_coin_high': 'Float64',
    'aion_coin_low': 'Float64',
    'aion_coin_open': 'Float64',
    'aion_coin_close': 'Float64',
    'aion_coin_marketcap': 'Float64',
    'aion_coin_volume': 'Float64',

}

table_dict['account_external_warehouse'] = {
    'block_timestamp': 'Datetime',
    'address': 'String',
    'timestamp_of_first_event': 'Datetime',
    'update_type':'String',
    'account_type': 'String',
    'amount': 'Float64',
    'transaction_cost': 'Float64',
    'block_time': 'Float64',
    'balance':'Float64',
    'difficulty': 'Float64',
    'mining_reward': 'Float64',
    'nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'hash_power': 'Float64',
    'year':'UInt16',
    'month':'UInt16',
    'day':'UInt16',
    'hour':'UInt16',

}

table_dict['account_ext_warehouse'] = {
    'block_timestamp': 'Datetime',
    'address': 'String',
    'timestamp_of_first_event': 'Datetime',
    'update_type':'String',
    'account_type': 'String',
    'status':'String',
    'amount': 'Float64',
    'transaction_cost': 'Float64',
    'block_time': 'Float64',
    'balance':'Float64',
    'difficulty': 'Float64',
    'mining_reward': 'Float64',
    'nrg_reward': 'Float64',
    'num_transactions': 'Float64',
    'hash_power': 'Float64',
    'year':'UInt16',
    'month':'UInt16',
    'day':'UInt16',
    'hour':'UInt16',

}

table_dict['crypto_daily'] = {
    'timestamp': 'Date',
    'crypto':'String',
    'watch': 'UInt64',
    'fork': 'UInt64',
    'issue': 'UInt64',
    'release': 'UInt64',
    'push': 'UInt64',
    'close': 'Float64',
    'high': 'Float64',
    'low': 'Float64',
    'market_cap': 'Float64',
    'volume': 'Float64',
    'sp_close': 'Float64',
    'sp_volume':'Float64',
    'russell_close':'Float64',
    'russell_volume':'Float64'
}
################################################################
#            MODEL FUNCTION
#################################################################
'''
def model(self, date):
    try:
        pass
    except Exception:
        logger.error('', exc_info=True)

''' '''
self.new_activity_lst = df.apply(create_address_transaction,
                                 args=(table,churned_addresses,self.new_activity_lst,),
                                 axis=1,meta=('address_lst','O'))
'''


#################################################################
#                   SOCIAL MEDIA HELPERS
################################################################
rename_dict = {}
rename_dict['twitter'] = {
    'aelf': 'aelfblockchain',
    'aion': 'aion_network',
    'ark': 'arkecosystem',
    'bitcoin-cash':'bitcoincash',
    'bitcoin_cash':'bitcoincash',
    'dash':'dashpay',
    'decred':'decredproject',
    'digibyte':'digibytecoin',
    'eos':'block_one_',
    'golem':'golemproject',
    'icon':'helloiconworld',
    'iota':'IOTA_crypto',
    'kucoin':'kucoincom',
    'lisk':'liskhq',
    'maker':'makerdao',
    'nem':'nemofficial',
    'neo':'neo_blockchain',
    'omisego':'omise_go',
    'ontology':'ontologynetwork',
    'pivx':'_pivx',
    'qtum':'qtumofficial',
    'siacoin':'siatechhq',
    'steem':'steemit',
    'stella':'StellarOrg',
    'theta':'theta_network',
    'tron':'tronfoundation',
    'wanchain': 'wanchain_org',
    'waves':'wavesplatform',
    'zcash':'zcashco',

























}