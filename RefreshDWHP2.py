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
import RecordTypeGLF
import MainframeFieldsUtility
import ElaborationSQLUtility
import Logger
import CommonUtility
import RefreshUtility
import Schema_eDWHP2
from time import gmtime, strftime
import sys


#if __name__ == '__main__':
def execute():
    esito = False

    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    CommonUtility.setHDFSproperties(sqlContext)

    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_DWHP2 = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_DWHP2')
    FTP_WORKED_PATH_DWHP2 = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_DWHP2')
    
    E_TABLE = 'loyalty_fca.e_dwhp2_cams'

    file_name_elaborazione = CommonUtility.get_filename_from_input_table(sqlContext, E_TABLE, 'input_filename')

    # pulizia tabella e_
    RefreshUtility.truncate_e_table(sqlContext, E_TABLE)
    # spostare file in worked
    RefreshUtility.move_file_to_worked(FTP_ROOT + FTP_INPUT_PATH_DWHP2 + file_name_elaborazione,
                                       FTP_ROOT + FTP_WORKED_PATH_DWHP2 + file_name_elaborazione)

    esito = True
    return esito
    