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
import Logger
from time import gmtime, strftime
import sys

def truncate_e_table(sqlContext, e_table):
    logger = Logger.getLogger()
    logger.info('Inizio truncate ' + e_table + '\n')
    sqlContext.sql("truncate table " + e_table)
    logger.info('Fine truncate ' + e_table + '\n')

def move_file_to_worked(source_path, destination_path):
    logger = Logger.getLogger()
    os.rename(source_path, destination_path)
    logger.info('Spostato file ' + source_path + ' in ' + destination_path)
