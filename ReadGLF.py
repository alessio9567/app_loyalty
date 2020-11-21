#!/usr/bin/env python

from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import datetime
import glob
import os
import FileUtility
import ReadUtility
import CommonUtility
import MainframeFieldsUtility
import RecordTypeGLF
import Logger
import Schema_eGLF
from time import gmtime, strftime



#if __name__ == '__main__':
def execute():
    esito = False
    
    logger = Logger.getLogger()
    logger.info('START ReadGLF.py')
    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext = CommonUtility.setHDFSproperties(sqlContext)
    
    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_GLF = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_GLF')
    FIXEDPART_GLF = CommonUtility.getParameterFromFile('FIXEDPART_GLF')
    LEN_TIMESTAMP_GLF = CommonUtility.getParameterFromFile('LEN_TIMESTAMP_GLF')
    HDFS_ROOT = CommonUtility.getParameterFromFile('HDFS_ROOT')
    INPUT_PATH_GLF = CommonUtility.getParameterFromFile('INPUT_PATH_GLF')
    FTP_WORKED_PATH_GLF = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_GLF')
    
    APPLICATION_CODE = 'CAMS'
    P_TABLE = 'loyalty_fca.p_glf_cams'
    E_TABLE = 'loyalty_fca.e_glf_cams'
    #CAUSALE_TRANSAZIONE_GENERICA = 'Virtual Credit per Transazione'
    CAUSALE_TRANSAZIONE_GENERICA = 'FCA Bank - Buoni sconto per utilizzo carta'
    CODICE_CAUSALE_TRANSAZIONE_GENERICA = 'GT'
    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())
    
    inputFile = FileUtility.getInputFilename(FTP_ROOT + FTP_INPUT_PATH_GLF, FIXEDPART_GLF, LEN_TIMESTAMP_GLF)
    
    elabFileAbsPath = inputFile.input_filename_AbsPath
    input_filename = inputFile.input_filename
    input_filename_timestamp = inputFile.input_filename_timestamp
    
    if(input_filename == ''):
        logger.warning('Nome file non valido o file non presente')
    else:
        file_duplicato = False
        file_duplicato = ReadUtility.inputFileDuplicateCheck(sqlContext, sc, P_TABLE, file_duplicato, input_filename)
        if not file_duplicato:
            
            logger.info('Lettura file elabFileAbsPath ' + elabFileAbsPath)
            fileHdfsAbsPath = HDFS_ROOT + INPUT_PATH_GLF + input_filename
            
            ReadUtility.copy_file_from_local_to_hdfs(elabFileAbsPath, HDFS_ROOT + INPUT_PATH_GLF)
            
            transaction = list()
            account_balance_18 = dict()
            account_balance_26 = dict()
            
            file_hadoop = sc.binaryFiles(fileHdfsAbsPath) # , use_unicode=False
            data_list = file_hadoop.map(lambda p: p)
            blob_array = data_list.collect()[0]
            blob = blob_array[1]
            blobr = blob.replace('\\\\', '\\')
            lista_righe = blobr.split('\r\n')
            
            loyalty_program_cod = ''
            internallog_ts = ''
            
            for row in lista_righe:
                
                if (row[0:2] == '06'):
                    fields = RecordTypeGLF.map_record_type_06(row)
                    CommonUtility.print_dictionary(fields)
                    internallog_ts = fields["internallog_ts"] # potrebbe essere usato come data riferimento del file
                elif(row[0:2] == '18'):
                    # il record di tipo 18 c e sempre, anche se a 0. In quel caso non ci saranno record con type 26
                    fields = RecordTypeGLF.map_record_type_18(row)
                    CommonUtility.print_dictionary(fields)
                    
                    account_balance_18[fields["LYL-PGM-ID"] + '_' + fields["SRT-AC-CD"]] = fields["virtual_credit_regular"]
                    #logger.info('Virtual Credit 18 ' + str(account_balance_18[fields["LYL-PGM-ID"] + '_' + fields["SRT-AC-CD"]]))
                    
                elif(row[0:2] == '26'):
                    fields = RecordTypeGLF.map_record_type_26(row)
                    CommonUtility.print_dictionary(fields)
                    
                    try:
                        account_balance_26[fields["LYL-PGM-ID"] + '_' + fields["SRT-AC-CD"]] += fields["virtual_credit_regular"]
                    except Exception as e:
                        account_balance_26[fields["LYL-PGM-ID"] + '_' + fields["SRT-AC-CD"]] = fields["virtual_credit_regular"]
                    
                    #logger.info('Virtual Credit 26 ' + str(account_balance_26[fields["LYL-PGM-ID"] + '_' + fields["SRT-AC-CD"]]))
                    
                    #row = Row(#{    
                    #        APPCODE =                   APPLICATION_CODE,
                    #        PRODUCT_COMPANY=            fields["SPS-AC-CO-NR"],
                    #        ACCOUNT_NUMBER=             fields["SRT-AC-CD"],
                    #        LOYALTY_PROGRAM_COD=        fields["LYL-PGM-ID"],
                    #        TRANSACTION_ID=             fields["JO-SYS-GEN-REF-NR"],
                    #        TRANSACTION_DATE=           fields["TXN-DT"],
                    #        TRANSACTION_VALUE=          fields["JO_PST_AM_VALORE"],
                    #        TRANSACTION_TYPE=           'VC',
                    #        SIGN=                       MainframeFieldsUtility.sign_Handler(fields["JO-REG-PNT-AM"])['segno'],
                    #        CURRENCY=                   fields["TXN-CURR-CD"],
                    #        VALUE=                      MainframeFieldsUtility.sign_Handler(fields["JO-REG-PNT-AM"])['campo'].lstrip('0'),
                    #        CAUSAL=                     CAUSALE_TRANSAZIONE_GENERICA,
                    #        CAUSALCODE=                 CODICE_CAUSALE_TRANSAZIONE_GENERICA,
                    #        INPUT_FILENAME=             input_filename,
                    #        INPUT_FILENAME_TIMESTAMP=   input_filename_timestamp,
                    #        ELABORATION_TS=             elaboration_ts,
                    #        INTERNALLOG_TS=             internallog_ts
                    #)#}
                    
                    row = [ 
                        APPLICATION_CODE,
                        fields["SPS-AC-CO-NR"],
                        fields["SRT-AC-CD"],
                        fields["LYL-PGM-ID"],
                        fields["JO-SYS-GEN-REF-NR"] + fields["CATG-CD"],
                        fields["transaction_date"],
                        fields["jo_pst_am_valore"], # valore della transazione (per ora usa lo stesso segno dei punti)
                        'VC',
                        fields["jo_reg_pnt_am_segno"], 
                        'EUR', # campo currency forzato a euro perche riferito a transazione convertita in euro
                        fields["jo_reg_pnt_am_valore"], 
                        CAUSALE_TRANSAZIONE_GENERICA,
                        CODICE_CAUSALE_TRANSAZIONE_GENERICA,
                        input_filename,
                        input_filename_timestamp,
                        elaboration_ts,
                        internallog_ts
                    ]
                    #logger.info(row)
                    transaction.append(row)
                    
                else:
                    logger.info("Record di tipo diverso " + row[0:2])
                    
            total_read_rows = len(transaction)
            logger.info('Lette ' + str(total_read_rows) + ' righe del file ' + FTP_ROOT + FTP_INPUT_PATH_GLF + input_filename)
            if total_read_rows > 0:
                schema_e_glf_cams = Schema_eGLF.getSchema()      
                try:
                    ReadUtility.fromListToHiveTablesDuplicateCheck(sqlContext, transaction, sc, schema_e_glf_cams, E_TABLE, P_TABLE, file_duplicato, internallog_ts)
                    if file_duplicato:
                        os.rename(FTP_ROOT + FTP_INPUT_PATH_GLF + input_filename,
                                  FTP_ROOT + FTP_WORKED_PATH_GLF + input_filename)
                        logger.warning("File GLF aveva internallog_ts duplicato nella tabella " + P_TABLE)
                    else:
                        esito = True
                except Exception as e:
                    logger.error("Fallita la scrittura delle tabelle " + E_TABLE + " e " + P_TABLE)
            else:
                os.rename(FTP_ROOT + FTP_INPUT_PATH_GLF + input_filename, FTP_ROOT + FTP_WORKED_PATH_GLF + input_filename)
                logger.warning("Nel file " + input_filename + " non ci sono record significativi, viene spostato in " + FTP_ROOT + FTP_WORKED_PATH_GLF) 
        else:
            os.rename(FTP_ROOT + FTP_INPUT_PATH_GLF + input_filename, FTP_ROOT + FTP_WORKED_PATH_GLF + input_filename)
            logger.warning("File GLF era presente nella tabella " + P_TABLE)
    logger.info('END ReadGLF.py')
    return esito
