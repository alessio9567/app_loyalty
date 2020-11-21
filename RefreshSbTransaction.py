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
import FileUtility
import CommonUtility
import RefreshUtility
import Schema_eSbTransaction
import Logger
from time import gmtime, strftime


#if __name__ == '__main__':
def execute():
    esito = False

    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    CommonUtility.setHDFSproperties(sqlContext)

    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_TRANSACTION_WINWINIT = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_TRANSACTION_WINWINIT')
    FTP_WORKED_PATH_TRANSACTION_WINWINIT = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_TRANSACTION_WINWINIT')

    E_TABLE = 'loyalty_fca.e_transaction_saveback'

    file_name_elaborazione = CommonUtility.get_filename_from_input_table(sqlContext, E_TABLE, 'input_filename')

    # pulizia tabella e_
    RefreshUtility.truncate_e_table(sqlContext, E_TABLE)
    # spostare file in worked
    RefreshUtility.move_file_to_worked(FTP_ROOT + FTP_INPUT_PATH_TRANSACTION_WINWINIT + file_name_elaborazione,
                                       FTP_ROOT + FTP_WORKED_PATH_TRANSACTION_WINWINIT + file_name_elaborazione)
    esito = True
    return esito
    
    
    
    