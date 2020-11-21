#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import HiveContext
import subprocess
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
import FileUtility
import Logger


logger = Logger.getLogger()


def fromListToHiveTables(rowList, sc, schema, e_tablename, p_tablename, file_duplicato):
    if(len(rowList) > 0):
        #logger.info(rowList)
        #        df_csv = csv_data.map(lambda p: Row(id = p[0], display = p[1], productId = p[2], brand=p[3], category=p[4]))
        logger.info('Creazione HiveContext')
        hc = HiveContext(sc)
        logger.info('Creazione DataFrame')
        df = hc.createDataFrame(rowList, schema=schema)
        if file_duplicato:
            logger.info('Il file è duplicato\n')
            #None.write.mode('overwrite').format("orc").saveAsTable(e_tablename)
            #sqlContext.sql("truncate table e_tablename")
        else:
            logger.info('Scrittura in overwrite della tabella ' + e_tablename)
            df.write.mode('overwrite').format("orc").saveAsTable(e_tablename)
            df.write.mode('append').format("orc").saveAsTable(p_tablename)
    else:
        logger.info("il file letto è vuoto o non ha record significativi, quindi la tabella " + e_tablename + " non viene scritta")
        
        
        
def fromListToHiveTablesDuplicateCheck(sqlContext, rowList, sc, schema, e_tablename, p_tablename, file_duplicato, internallog_ts):

    df_internallog = sqlContext.sql("SELECT distinct internallog_ts as internallog_ts " \
                              " from " + p_tablename + \
                              " where 1=1" \
                              "" \
                              )
    # testare bene internallog
    file_duplicato = (file_duplicato or FileUtility.is_duplicate_file( df_internallog, "internallog_ts", internallog_ts))
    fromListToHiveTables(rowList, sc, schema, e_tablename, p_tablename, file_duplicato)
    



def inputFileDuplicateCheck(sqlContext, sc, p_tablename, file_duplicato, input_filename):

    df_filename = sqlContext.sql("SELECT distinct input_filename as input_filename " \
                                          " from " + p_tablename + " where 1=1" \
                                          "" \
                                          )
    file_duplicato = (file_duplicato or FileUtility.is_duplicate_file( df_filename, "input_filename", input_filename ))
    return file_duplicato
    
def copy_file_from_local_to_hdfs(elabFileAbsPath, hdfs_path):
    logger.info('Copia il file ' + elabFileAbsPath + ' su hdfs nel path ' + hdfs_path + '\n ')
    subprocess.call(["hadoop", "dfs", "-copyFromLocal", elabFileAbsPath, hdfs_path])




