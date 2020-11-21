#!/usr/bin/env python

def getSchema():
    schema_TRANSACTION_WINWINIT = [
        'appcode',
        'product_company',
        'customer_number',
        'transaction_id',
        'transaction_date',
        'transaction_type',
        'sign',
        'currency',
        'value',
        'causal',
        'causalcode',
        'input_filename',
        'input_filename_timestamp',
        'elaboration_ts',
        'internallog_ts']
    return schema_TRANSACTION_WINWINIT
