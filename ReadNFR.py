
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
import RecordTypeNFR
import Logger
import Schema_eNFR
from time import gmtime, strftime

#if __name__ == '__main__':
def execute():
    esito = False
    
    logger = Logger.getLogger()
    logger.info('START ReadNFR.py')
    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    sqlContext = CommonUtility.setHDFSproperties(sqlContext)
    
    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_NFR = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_NFR')
    FIXEDPART_NFR = CommonUtility.getParameterFromFile('FIXEDPART_NFR')
    LEN_TIMESTAMP_NFR = CommonUtility.getParameterFromFile('LEN_TIMESTAMP_NFR')
    HDFS_ROOT = CommonUtility.getParameterFromFile('HDFS_ROOT')
    INPUT_PATH_NFR = CommonUtility.getParameterFromFile('INPUT_PATH_NFR')
    FTP_WORKED_PATH_NFR = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_NFR')
    
    APPLICATION_CODE = 'CAMS'
    P_TABLE = 'loyalty_fca.p_nfr_cams'
    E_TABLE = 'loyalty_fca.e_nfr_cams'
    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())
    internallog_ts = ''
    
    inputFile = FileUtility.getInputFilename(FTP_ROOT + FTP_INPUT_PATH_NFR, FIXEDPART_NFR, LEN_TIMESTAMP_NFR)
    
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
            fileHdfsAbsPath = HDFS_ROOT + INPUT_PATH_NFR + input_filename
            
            ReadUtility.copy_file_from_local_to_hdfs(elabFileAbsPath, HDFS_ROOT + INPUT_PATH_NFR)
            
            validKey = RecordTypeNFR.getNFRvalidKeys()
            
            ### analizzare per IBRkey!!!!!!!!
            
            #file_hadoop = sc.textFile(elabFileAbsPath)
            #data_list = file_hadoop.map(lambda p: p)
            
            #firstLine = data_list.first()
            #linesSkipFirst = data_list.filter(lambda p:p != firstLine)    
            
            file_hadoop = sc.binaryFiles(fileHdfsAbsPath) # , use_unicode=False
            #file_hadoop_text = sc.textFile(elabFileAbsPath, use_unicode=False) # , use_unicode=False
            data_list = file_hadoop.map(lambda p: p)
            #data_list_text = file_hadoop_text.map(lambda p: p)
            blob_array = data_list.collect()[0]
            blob = blob_array[1]
            blobr = blob.replace('\\\\', '\\')
            lista_righe = blobr.split('\r\n')
            #for row in data_list_text.collect():
            firstLine = lista_righe[0]
            linesSkipFirst = lista_righe[1:]
            
            customersREQCARD = dict()
            customersLOYADD = dict()
            customersLOYDEL = dict()
            
            if RecordTypeNFR.isFileHeader(firstLine):
                fields = RecordTypeNFR.mapHeaderRow(firstLine)
                internallog_ts = fields["batchID"]
                recordCounter = fields["recordCounter"]
                concat = internallog_ts + ';' + recordCounter
                #for row in linesSkipFirst.collect():
                for row in linesSkipFirst:
                    fields = RecordTypeNFR.mapDetailRow(row)
                    if(fields["functionID"] == 'REQCARD'):
                        if (fields["result"] == 'P' and fields["aliasID"] in validKey):
                            concat = fields["issuerBatchId"] + ' ' + fields["ibrKey"] + ' ' + fields["keyOfFunction"] + ' ' + fields["keyType"] + ' ' + fields["aliasID"] + ' ' + fields["value"]
                            customersREQCARD = RecordTypeNFR.setCustomerProperties( customersREQCARD, fields["keyOfFunction"], fields["aliasID"], fields["value"] )
                            customersREQCARD = RecordTypeNFR.setCustomerProperties( customersREQCARD, fields["keyOfFunction"], "issuerBatchId", fields["issuerBatchId"] )
                            #customers[fields["ibrKey"]] = customerProperties #valutare questa cosa, meglio usare keyOfFunction?
                    elif(fields["functionID"] == 'LOYDEL'):   
                        if (fields["result"] == 'P' and fields["keyType"] == 'A'):
                            # il test prevede che vengano presi solo i record relativi all accountNumber
                             # viene Testato che fields["keyOfFunction"] == fields["value"] perche in entrambi dovrebbe essere l'accountNumber
                            if(fields["aliasID"] == 'X1142237'):
                                if(fields["aliasID"] == 'X1142237' and fields["keyOfFunction"] == fields["value"]):
                                    customersLOYDEL = RecordTypeNFR.setCustomerProperties( customersLOYDEL, fields["keyOfFunction"], fields["aliasID"], fields["value"] )
                                else:
                                    logger.error("ERRORE GRAVE! SEGNALARE SCARTO " + fields["keyOfFunction"] + "diverso da " + fields["value"])
                    elif(fields["functionID"] == 'LOYADD'):  
                        if (fields["result"] == 'P' and fields["keyType"] == 'A'):
                            # il test prevede che vengano presi solo i record relativi all accountNumber
                            # viene Testato che fields["keyOfFunction"] == fields["value"] perche in entrambi dovrebbe essere l'accountNumber
                            if(fields["aliasID"] == 'X1142237'):
                                if(fields["aliasID"] == 'X1142237' and fields["keyOfFunction"] == fields["value"]):
                                    customersLOYADD = RecordTypeNFR.setCustomerProperties( customersLOYADD, fields["keyOfFunction"], fields["aliasID"], fields["value"] )
                                else:
                                    logger.error("ERRORE GRAVE! SEGNALARE SCARTO " + fields["keyOfFunction"] + "diverso da " + fields["value"])
                            elif(fields["aliasID"] == 'X11422FS'): #loyaltyProgram
                                customersLOYADD = RecordTypeNFR.setCustomerProperties( customersLOYADD, fields["keyOfFunction"], fields["aliasID"], fields["value"] )
                            elif(fields["aliasID"] == 'X11422BI'): #loyaltyStatusCode
                                customersLOYADD = RecordTypeNFR.setCustomerProperties( customersLOYADD, fields["keyOfFunction"], fields["aliasID"], fields["value"] )
            else : 
                logger.error("Error! First line is not the header!")
                
            rows = RecordTypeNFR.setNfrTable( customersREQCARD , APPLICATION_CODE, input_filename, input_filename_timestamp, elaboration_ts, internallog_ts, 'L')
            
            total_read_rows = len(rows)
            logger.info( 'Lette ' + str(total_read_rows) + ' righe del file ' + FTP_ROOT + FTP_INPUT_PATH_NFR + input_filename)
            if total_read_rows > 0:
                schema_e_nfr_cams = Schema_eNFR.getSchema()
                try:
                    ReadUtility.fromListToHiveTablesDuplicateCheck(sqlContext, rows, sc, schema_e_nfr_cams, E_TABLE, P_TABLE, file_duplicato, internallog_ts)
                    esito = True  
                except Exception as e:
                    logger.error("Fallita la scrittura delle tabelle " + E_TABLE + " e " + P_TABLE)
            else:
                os.rename(FTP_ROOT + FTP_INPUT_PATH_NFR + input_filename, FTP_ROOT + FTP_WORKED_PATH_NFR + input_filename)
                logger.warning("Nel file " + input_filename + " non ci sono record significativi, viene spostato in " + FTP_ROOT + FTP_WORKED_PATH_NFR)           
        else:
            os.rename(FTP_ROOT + FTP_INPUT_PATH_NFR + input_filename, FTP_ROOT + FTP_WORKED_PATH_NFR + input_filename)
            logger.warning("File NFR era presente nella tabella " + P_TABLE)
            
    logger.info('END ReadNFR.py')
    
    return esito
