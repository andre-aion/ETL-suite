checkpoint_dict = {}
table = 'block_tx_warehouse'
checkpoint_dict[table] = {
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}

table = 'network_activity'
checkpoint_dict[table] ={
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}

table = 'account_balance'
checkpoint_dict[table] ={
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}

table = 'account_activity'
checkpoint_dict[table] ={
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}


table = 'account_activity_churn'
checkpoint_dict[table] ={
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}

table = 'account_value_churn'
checkpoint_dict[table] ={
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}

table = 'account_activity_warehouse'
checkpoint_dict[table] ={
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}

table = 'coinscraper'
checkpoint_dict[table] ={
    'table' : table,
    'column':'date',
    'items':{}
}

table = 'indexscraper'
checkpoint_dict[table] ={
    'table' : table,
    'column':'date',
    'items':{}
}

table = 'github'
checkpoint_dict[table] ={
    'table' : table,
    'column':'date',
    'items':{}
}

table = 'account_external_warehouse'
checkpoint_dict[table] ={
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}

table = 'account_ext_warehouse'
checkpoint_dict[table] ={
    'table' : table,
    'column':'timestamp',
    'offset':None,
    'timestamp' : None,
    'start':None,
    'end':None
}

table = 'crypto_daily'
checkpoint_dict[table] ={
    'table' : table,
    'column':'timestamp',
    'offset':None,
    'timestamp' : None,
    'start':None,
    'end':None
}

table = 'country_economic_indicators'
checkpoint_dict[table] ={
    'table' : table,
    'column':'timestamp',
    'offset':None,
    'timestamp' : None,
    'start':None,
    'end':None
}

table = 'twitterscraper'
checkpoint_dict[table] ={
    'table' : table,
    'column':'timestamp',
    'offset':None,
    'timestamp' : None,
    'start':None,
    'end':None
}