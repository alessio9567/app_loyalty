#!/usr/bin/env python

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
import Logger
import Schema_eSbTransaction
from time import gmtime, strftime

#if __name__ == '__main__':
def execute():
    esito = False
    
    logger = Logger.getLogger()
    logger.info('START ReadSbTransaction.py')
    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext = CommonUtility.setHDFSproperties(sqlContext)

    #######################################
    #######################################

    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_TRANSACTION_WINWINIT = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_TRANSACTION_WINWINIT')
    FIXEDPART_TRANSACTION_WINWINIT_INPUT = CommonUtility.getParameterFromFile('FIXEDPART_TRANSACTION_WINWINIT_INPUT')
    LEN_TIMESTAMP_TRANSACTION_WINWINIT_INPUT = CommonUtility.getParameterFromFile('LEN_TIMESTAMP_TRANSACTION_WINWINIT_INPUT')
    HDFS_ROOT = CommonUtility.getParameterFromFile('HDFS_ROOT')
    INPUT_PATH_TRANSACTION_WINWINIT = CommonUtility.getParameterFromFile('INPUT_PATH_TRANSACTION_WINWINIT')
    FTP_WORKED_PATH_TRANSACTION_WINWINIT = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_TRANSACTION_WINWINIT')

 
    APPLICATION_CODE = 'SB'
    P_TABLE = 'loyalty_fca.p_transaction_saveback'
    E_TABLE = 'loyalty_fca.e_transaction_saveback'
    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())
    internallog_ts = ''

    inputFile = FileUtility.getInputFilename(FTP_ROOT + FTP_INPUT_PATH_TRANSACTION_WINWINIT, FIXEDPART_TRANSACTION_WINWINIT_INPUT, LEN_TIMESTAMP_TRANSACTION_WINWINIT_INPUT)

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
            fileHdfsAbsPath = HDFS_ROOT + INPUT_PATH_TRANSACTION_WINWINIT + input_filename    

            ReadUtility.copy_file_from_local_to_hdfs(elabFileAbsPath, HDFS_ROOT + INPUT_PATH_TRANSACTION_WINWINIT)
        
            schema_TRANSACTION_WINWINIT = Schema_eSbTransaction.getSchema()
        
            fileCsv = FileUtility.custom_read_csv(sc, schema=schema_TRANSACTION_WINWINIT, header=False, mode='DROPMALFORMED', delimiter=';', path=fileHdfsAbsPath, spark_version=1)

            total_read_rows = len(fileCsv)
            logger.info('Lette ' + str(total_read_rows) + ' righe del file ' + FTP_ROOT + FTP_INPUT_PATH_TRANSACTION_WINWINIT + input_filename)
            if total_read_rows > 0:

                fileCsvEnrich = list()
                for singleArray in fileCsv:
                    fileCsvEnrich.append(singleArray[0:11] + [input_filename, input_filename_timestamp, elaboration_ts] + singleArray[11:12])          
                try:
                    ReadUtility.fromListToHiveTables(fileCsvEnrich, sc, schema_TRANSACTION_WINWINIT, E_TABLE, P_TABLE, file_duplicato)
                    esito = True
                except Exception as e:
                    logger.error("Fallita la scrittura delle tabelle " + E_TABLE + " e " + P_TABLE)
            else:
                os.rename(FTP_ROOT + FTP_INPUT_PATH_TRANSACTION_WINWINIT + input_filename, FTP_ROOT + FTP_WORKED_PATH_TRANSACTION_WINWINIT + input_filename)
                logger.warning("Nel file " + input_filename + " non ci sono record significativi, viene spostato in " + FTP_ROOT + FTP_WORKED_PATH_TRANSACTION_WINWINIT)     
        else:
            os.rename(FTP_ROOT + FTP_INPUT_PATH_TRANSACTION_WINWINIT + input_filename, FTP_ROOT + FTP_WORKED_PATH_TRANSACTION_WINWINIT + input_filename)
            logger.warning("File GLF era presente nella tabella " + P_TABLE)
    
    logger.info('END ReadSbTransaction.py')
    return esito