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

table = 'githubloader'
checkpoint_dict[table] ={
    'table' : table,
    'column':'date',
    'items':{}
}