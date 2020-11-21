#!/usr/bin/env python

from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import Schema_eGLF

if __name__ == '__main__':
    conf = SparkConf().setAppName("app")
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)
    schema = Schema_eGLF.getSchema()

    fileHdfsAbsPath = 'hdfs://ftpandbit01.carte.local/apps/hive/warehouse/loyalty_fca/e_glf_cams_estemporanea'
    myrdd = sc.textFile(fileHdfsAbsPath).map(lambda line: line.encode('utf-8').split(';')).filter(
        lambda line: len(line) > 1)
    mdf = sqlContext.createDataFrame(myrdd, schema)
    print('\n conteggio record dataframe ' + str(mdf.count()))

    mdf.write.mode('overwrite').format("orc").saveAsTable('loyalty_fca.e_glf_cams')
