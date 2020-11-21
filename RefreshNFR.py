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
import subprocess
import ElaborationSQLUtility
import FileUtility
import CommonUtility
import RefreshUtility
import Schema_eNFR
from time import gmtime, strftime
import sys
import Logger

#if __name__ == '__main__':
def execute():
    esito = False
    
    reload(sys)
    sys.setdefaultencoding('utf8')
    
    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    CommonUtility.setHDFSproperties(sqlContext)
    
    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())
    
    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_NFR = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_NFR')
    FTP_WORKED_PATH_NFR = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_NFR')
    
    E_TABLE = 'loyalty_fca.e_nfr_cams'
    
    file_name_elaborazione = CommonUtility.get_filename_from_input_table(sqlContext, E_TABLE, 'input_filename')
    # pulizia tabella e_
    RefreshUtility.truncate_e_table(sqlContext, E_TABLE)
    # spostare file nfr in worked   
    RefreshUtility.move_file_to_worked(FTP_ROOT + FTP_INPUT_PATH_NFR + file_name_elaborazione,
                                       FTP_ROOT + FTP_WORKED_PATH_NFR + file_name_elaborazione)
    esito = True
    
    return esito
