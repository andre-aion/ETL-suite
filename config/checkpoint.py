checkpoint_dict = {}
table = 'block_tx_warehouse'
checkpoint_dict[table] = {
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}

table = 'miner_activity'
checkpoint_dict[table] ={
    'table' : table,
    'column':'block_timestamp',
    'offset':None,
    'timestamp' : None
}