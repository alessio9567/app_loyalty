#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Logger
from pyspark.sql import Row

logger = Logger.getLogger()

def mapDetailRow( stringa ):
        row = {
            "KEY_CAMS_COID" : stringa[0:5].encode('utf-8').strip(),
            "KEY_TYPE" : stringa[5:6].encode('utf-8').strip(), # valori possibili C A P
            "KEY_ID" : stringa[6:29].encode('utf-8').strip(),
            "CRT_TS" : stringa[29:55].encode('utf-8').strip(), 
            "USER_ID" : stringa[55:63].encode('utf-8').strip(), # non utilizzato da Ubiq
            "MAINT_TS" : stringa[63:89].encode('utf-8').strip(), # non utilizzato da Ubiq
            "ALIAS" : stringa[89:97].encode('utf-8').strip(),
            "MAINT_TYPE" : stringa[97:98].encode('utf-8').strip(), # valori possibili C A D
            "OLD_VALUE" : stringa[98:598].encode('utf-8').strip(),
            "NEW_VALUE" : stringa[598:1098].encode('utf-8').strip().rstrip('\n').rstrip('\r\n')
        }
        return row

def getDictKey(dizionario, chiave):
    if(chiave in dizionario.keys()):
        return dizionario[chiave]
    else:
        return ''

def setTable(accounts, APPLICATION_CODE, product_company, input_filename, input_filename_timestamp, elaboration_ts, internallog_ts, RowOrList = 'L'):
    rowList = list()
    rowRow = list()
    for accountNumber in accounts.keys():
        if getDictKey(accounts[accountNumber],'X11422C2') == '':
            rowList.append((
            APPLICATION_CODE
            , product_company # 18200
            , 'A'
            , 'customerNumberNA' 
            , accountNumber   
            , '' # CUSTMER STATUS
            , '' # ACCOUNT STATUS
            , input_filename
            , input_filename_timestamp
            , elaboration_ts
            , internallog_ts
            , getDictKey(accounts[accountNumber],'X1142209')#STATO_CARTA
            , getDictKey(accounts[accountNumber],'X11422PG')#CUP
            , getDictKey(accounts[accountNumber],'X1142289')#LIVREA
            , getDictKey(accounts[accountNumber],'X1142278')#DEALER
            , getDictKey(accounts[accountNumber],'X1142234')))#CONVENZIONE
            rowRow.append(Row(
            appcode =APPLICATION_CODE
            , product_company = product_company # 18200
            , keytype ='A' # getDictKey(accounts[accountNumber], 'X11422PC') 
            , customer_number = 'customerNumberNA' 
            , account_number = accountNumber
            , customer_status = '' # stesso dell account
            , account_status = ''
            , input_filename = input_filename
            , input_filename_timestamp = input_filename_timestamp
            , elaboration_ts = elaboration_ts
            , internallog_ts = internallog_ts
            , plastic_status = getDictKey(accounts[accountNumber],'X1142209')
            , cup = getDictKey(accounts[accountNumber],'X11422PG')
            , livrea = getDictKey(accounts[accountNumber],'X1142289')
            , dealer= getDictKey(accounts[accountNumber],'X1142278')
            , convenzione = getDictKey(accounts[accountNumber],'X1142234')))
        else:
            rowList.append((
            APPLICATION_CODE
            , product_company # 18200
            , 'A'
            , 'customerNumberNA' 
            , accountNumber   
            , '' # CUSTMER STATUS
            , '' # ACCOUNT STATUS
            , input_filename
            , input_filename_timestamp
            , elaboration_ts
            , internallog_ts
            , getDictKey(accounts[accountNumber],'X11422C2')#STATO_CARTA
            , getDictKey(accounts[accountNumber],'X11422PG')#CUP
            , getDictKey(accounts[accountNumber],'X1142289')#LIVREA
            , getDictKey(accounts[accountNumber],'X1142278')#DEALER
            , getDictKey(accounts[accountNumber],'X1142234')))#CONVENZIONE
            rowRow.append(Row(
            appcode =APPLICATION_CODE
            , product_company = product_company # 18200
            , keytype ='A' # getDictKey(accounts[accountNumber], 'X11422PC') 
            , customer_number = 'customerNumberNA' 
            , account_number = accountNumber
            , customer_status = '' # stesso dell account
            , account_status = ''
            , input_filename = input_filename
            , input_filename_timestamp = input_filename_timestamp
            , elaboration_ts = elaboration_ts
            , internallog_ts = internallog_ts
            , plastic_status = getDictKey(accounts[accountNumber],'X11422C2')
            , cup = getDictKey(accounts[accountNumber],'X11422PG')
            , livrea = getDictKey(accounts[accountNumber],'X1142289')
            , dealer= getDictKey(accounts[accountNumber],'X1142278')
            , convenzione = getDictKey(accounts[accountNumber],'X1142234')))
    if RowOrList == 'R':
        return rowRow
    else:
        return rowList

