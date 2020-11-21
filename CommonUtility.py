from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import os
import subprocess
import Logger

logger = Logger.getLogger()

def getParameterFromFile(parameterName):
    with open("/home/hdfs/lbd/loyaltyFcaBank/Parametri/param_loyalty_fcab.txt") as f:
        lines = f.readlines()
        for line in lines:
            cleanLine = line.encode('utf-8').rstrip('\n').rstrip('\r\n').strip()
            arrL = cleanLine.split("=")
            if arrL[0] == parameterName:
                logger.info("Trovato parametro: " + parameterName + " con valore " + arrL[1])
                return arrL[1]
            #else:
                #logger.debug("Parametro " + parameterName + " non trovato")
        logger.error("Parametro " + parameterName + " non trovato")
        return ''


def print_dictionary( fields ):
    for key in fields.keys():
        print (key + ' :  ' + str(fields[key]))



def setHDFSproperties(sqlContext):
    #sqlContext.sql("set fs.defaultFS=hdfs://tfcdhdrclu-ns/")
    #sqlContext.sql("set hive.metastore.uris=thrift://tfbdadrnode04.gpaprod.local:9083,thrift://tfbdadrnode05.gpaprod.local:9083,thrift://tfbdadrnode06.gpaprod.local:9083")
    #sqlContext.sql("set hive.zookeeper.quorum=tfbdadrnode01.gpaprod.local,tfbdadrnode02.gpaprod.local,tfbdadrnode03.gpaprod.local")
    #sqlContext.sql("set hbase.zookeeper.quorum=tfbdadrnode01.gpaprod.local,tfbdadrnode02.gpaprod.local,tfbdadrnode03.gpaprod.local")
    #sqlContext.sql("set zookeeper.znode.parent=/hbase")
    return sqlContext



def get_filename_from_input_table(sqlContext, schema_table, filename_column):
       
    df_filename = sqlContext.sql("""SELECT distinct """
                                             + filename_column + """ as input_filename
                                            from """ + schema_table +
                                            """ where 1=1
                                            limit 1
                                           """ 
                                          )
    array_filename = df_filename.select(filename_column).rdd.flatMap(lambda x: x).collect()
    filename = ''
    if(len(array_filename) > 0):
        filename = str(array_filename[0])
    else:
        logger.warning("filename nella tabella " + schema_table + " non trovato")
        
    return filename
