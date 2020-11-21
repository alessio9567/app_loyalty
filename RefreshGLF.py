#!/usr/bin/env python

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import CommonUtility
import RefreshUtility

#if __name__ == '__main__':
def execute():
    esito = False
    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_GLF = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_GLF')
    FTP_WORKED_PATH_GLF = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_GLF')
    E_TABLE = 'loyalty_fca.e_glf_cams'
    SPARK_SQL_WAREHOUSE_DIR = CommonUtility.getParameterFromFile('SPARK_SQL_WAREHOUSE_DIR')
    SPARK_HADOOP_FS_DEFAULTFS = CommonUtility.getParameterFromFile('SPARK_HADOOP_FS_DEFAULTFS')

    spark = SparkSession \
        .builder \
        .appName("app") \
        .config("spark.sql.warehouse.dir", SPARK_SQL_WAREHOUSE_DIR) \
        .config("spark.hadoop.fs.defaultFS", SPARK_HADOOP_FS_DEFAULTFS) \
        .enableHiveSupport() \
        .getOrCreate()
    CommonUtility.setHDFSproperties(spark)

    file_name_elaborazione = CommonUtility.get_filename_from_input_table(spark, E_TABLE, 'input_filename')

    # pulizia tabella e_
    RefreshUtility.truncate_e_table(spark, E_TABLE)
    # spostare file in worked
    RefreshUtility.move_file_to_worked(FTP_ROOT + FTP_INPUT_PATH_GLF + file_name_elaborazione,
                                       FTP_ROOT + FTP_WORKED_PATH_GLF + file_name_elaborazione)

    esito = True
    return esito
