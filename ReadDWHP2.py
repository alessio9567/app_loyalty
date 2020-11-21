
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
import datetime
import glob
import os
import FileUtility
import ReadUtility
import CommonUtility
import RecordTypeDWHP2
import Logger
import Schema_eDWHP2
from time import gmtime, strftime

def execute():
    esito = False
    
    logger = Logger.getLogger()
    logger.info('START ReadDWHP2.py')
    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext = CommonUtility.setHDFSproperties(sqlContext)
    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_DWHP2 = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_DWHP2')
    FIXEDPART_DWHP2 = CommonUtility.getParameterFromFile('FIXEDPART_DWHP2')
    LEN_TIMESTAMP_DWHP2 = CommonUtility.getParameterFromFile('LEN_TIMESTAMP_DWHP2')
    HDFS_ROOT = CommonUtility.getParameterFromFile('HDFS_ROOT')
    INPUT_PATH_DWHP2 = CommonUtility.getParameterFromFile('INPUT_PATH_DWHP2')
    FTP_WORKED_PATH_DWHP2 = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_DWHP2')
    
    APPLICATION_CODE = 'CAMS'
    P_TABLE = 'loyalty_fca.p_dwhp2_cams'
    E_TABLE = 'loyalty_fca.e_dwhp2_cams'
    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())
    internallog_ts = None
    product_company = '18200'
    
    inputFile = FileUtility.getInputFilename(FTP_ROOT + FTP_INPUT_PATH_DWHP2, FIXEDPART_DWHP2, LEN_TIMESTAMP_DWHP2)
    
    elabFileAbsPath = inputFile.input_filename_AbsPath
    input_filename = inputFile.input_filename
    input_filename_timestamp = inputFile.input_filename_timestamp
    # internallog_ts forzato con il timestamp del file
    internallog_ts = input_filename_timestamp
    
    if input_filename == '':
        logger.warning('Nome file non valido o file non presente')
    else:    
        file_duplicato = False
        file_duplicato = ReadUtility.inputFileDuplicateCheck(sqlContext, sc, P_TABLE, file_duplicato, input_filename)
        
        if not file_duplicato:
            
            logger.info('Lettura file elabFileAbsPath ' + elabFileAbsPath)
            fileHdfsAbsPath = HDFS_ROOT + INPUT_PATH_DWHP2 + input_filename
            
            ReadUtility.copy_file_from_local_to_hdfs(elabFileAbsPath, HDFS_ROOT + INPUT_PATH_DWHP2)
            
            file_hadoop = sc.textFile(fileHdfsAbsPath)
            data_list = file_hadoop.map(lambda p: p)
            
            accounts_old = dict()
            subaccounts_old = dict()
            accounts_numbers = list()
            lista_di_account = list()
            
            for row in data_list.collect():
                fields = RecordTypeDWHP2.mapDetailRow(row)
                if fields["KEY_ID"] not in accounts_numbers:
                    accounts_numbers.append(fields["KEY_ID"])
            
            for i in accounts_numbers:
                for row in data_list.collect():
                    fields = RecordTypeDWHP2.mapDetailRow(row)
                    if fields["KEY_ID"] == i:
                        if fields["ALIAS"] == 'X11422C2':# or (fields["ALIAS"] == 'X1142209' and fields["KEY_TYPE"] == 'A'):#STATO CARTA
                            subaccounts_old[fields["ALIAS"]] = fields["OLD_VALUE"][0:2]
                            accounts_old[fields["KEY_ID"]] = subaccounts_old
                        if fields["ALIAS"] == 'X11422PG' and fields["MAINT_TYPE"] == 'C':#CUP (FISSO)
                            subaccounts_old[fields["ALIAS"]] = fields["OLD_VALUE"][0:15].strip()
                            accounts_old[fields["KEY_ID"]] = subaccounts_old
                        if fields["ALIAS"] == 'X1142289' and fields["MAINT_TYPE"] == 'A':#LIVREA (FISSO)
                            subaccounts_old[fields["ALIAS"]] = fields["OLD_VALUE"][0:3].strip()
                            accounts_old[fields["KEY_ID"]] = subaccounts_old
                        if fields["ALIAS"] == 'X1142278' and fields["MAINT_TYPE"] == 'A':#DEALER (FISSO)
                            subaccounts_old[fields["ALIAS"]] = fields["OLD_VALUE"][0:25]
                            accounts_old[fields["KEY_ID"]] = subaccounts_old
                        if fields["ALIAS"] == 'X1142234' and fields["MAINT_TYPE"] == 'A':#CONVENZIONE (FISSO)
                            subaccounts_old[fields["ALIAS"]] = fields["OLD_VALUE"][0:25].strip()
                            accounts_old[fields["KEY_ID"]] = subaccounts_old
                        if fields["KEY_CAMS_COID"]!= '18200':
                            logger.error("errore nella product_company!! dell account " + fields["KEY_ID"] + " e ha valore " + fields["KEY_CAMS_COID"])
                            logger.info(fields["KEY_ID"] + ' ' + fields["OLD_VALUE"][0:2])
                subaccounts_old= dict()
                lista_di_account.append(accounts_old)
            
            accounts_old=lista_di_account[0]
            
            accounts_new = dict()
            subaccounts_new= dict()
            lista_di_account=list()
            
            for i in accounts_numbers:
                for row in data_list.collect():
                    fields = RecordTypeDWHP2.mapDetailRow(row)
                    if fields["KEY_ID"]==i:
                        if fields["ALIAS"]=='X11422C2':# or (fields["ALIAS"] == 'X1142209' and fields["KEY_TYPE"] == 'A'):#STATO CARTA
                            subaccounts_new[fields["ALIAS"]]=fields["NEW_VALUE"][0:2]
                            accounts_new[fields["KEY_ID"]] = subaccounts_new
                        if fields["ALIAS"]=='X11422PG' and fields["MAINT_TYPE"] == 'C':#CUP (FISSO)
                            subaccounts_new[fields["ALIAS"]]=fields["NEW_VALUE"][0:15].strip()
                            accounts_new[fields["KEY_ID"]] = subaccounts_new
                        if fields["ALIAS"] == 'X1142289' and fields["MAINT_TYPE"] == 'A':#LIVREA (FISSO)
                            subaccounts_new[fields["ALIAS"]]=fields["NEW_VALUE"][0:3].strip()
                            accounts_new[fields["KEY_ID"]] = subaccounts_new
                        if fields["ALIAS"] == 'X1142278' and fields["MAINT_TYPE"] == 'A':#DEALER (FISSO)
                            subaccounts_new[fields["ALIAS"]]=fields["NEW_VALUE"][0:25]
                            accounts_new[fields["KEY_ID"]] =subaccounts_new
                        if fields["ALIAS"]=='X1142234' and fields["MAINT_TYPE"]=='A':#CONVENZIONE (FISSO)
                            subaccounts_new[fields["ALIAS"]]=fields["NEW_VALUE"][0:25].strip()
                            accounts_new[fields["KEY_ID"]] = subaccounts_new
                        if fields["KEY_CAMS_COID"]!= '18200':
                            logger.error("errore nella product_company!! dell account " + fields["KEY_ID"] + " e ha valore " + fields["KEY_CAMS_COID"])
                            logger.info(fields["KEY_ID"] + ' ' + fields["NEW_VALUE"][0:2])
                subaccounts_new= dict()
                lista_di_account.append(accounts_new)
            
            accounts_new=lista_di_account[0]
            
            rows_new = RecordTypeDWHP2.setTable( accounts_new , APPLICATION_CODE, product_company, input_filename, input_filename_timestamp, elaboration_ts, internallog_ts, 'L')
            
            rows_old = RecordTypeDWHP2.setTable( accounts_old , APPLICATION_CODE, product_company, input_filename, input_filename_timestamp, elaboration_ts, internallog_ts, 'L')
            
            rows_final=list()
            for i in range(len(rows_old)):
                rows_final.append((rows_new[i][0],rows_new[i][1],rows_new[i][2],rows_new[i][3],rows_new[i][4],rows_new[i][5],rows_new[i][6],rows_new[i][7],rows_new[i][8],rows_old[i][9],rows_new[i][10],\
                rows_old[i][11],rows_new[i][11],\
                rows_old[i][12],rows_new[i][12],\
                rows_old[i][13],rows_new[i][13],\
                rows_old[i][14],rows_new[i][14],\
                rows_old[i][15],rows_new[i][15]))
            
            total_read_rows = len(rows_final)
            logger.info('Lette ' + str(total_read_rows) + ' righe del file ' + FTP_ROOT + FTP_INPUT_PATH_DWHP2 + input_filename)
            if total_read_rows > 0:
                schema_e_dwhp2_cams = Schema_eDWHP2.getSchema()
                try:
                    ReadUtility.fromListToHiveTables(rows_final,sc,schema_e_dwhp2_cams,E_TABLE,P_TABLE, file_duplicato)
                    esito = True
                except Exception as e:
                    logger.error("Fallita la scrittura delle tabelle " + E_TABLE + " e " + P_TABLE) 
            else:
                os.rename(FTP_ROOT + FTP_INPUT_PATH_DWHP2 + input_filename, FTP_ROOT + FTP_WORKED_PATH_DWHP2 + input_filename)
                logger.warning("Nel file " + input_filename + " non ci sono record significativi, viene spostato in " + FTP_ROOT + FTP_WORKED_PATH_DWHP2) 
        else:
            os.rename(FTP_ROOT + FTP_INPUT_PATH_DWHP2 + input_filename, FTP_ROOT + FTP_WORKED_PATH_DWHP2 + input_filename)
            logger.warning("File DWHP2 era presente nella tabella " + P_TABLE)
        
    logger.info('END ReadDWHP2.py')
    return esito
