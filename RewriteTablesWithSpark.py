from pyspark.sql import SparkSession
from loyalty_fca_utility import CommonUtility

if __name__ == '__main__':
    def tabella_formato_spark(spark, table_name):
        print('Inizio riscrittura in formato ORC-SPARK della tabella ' + table_name)
        df_temp = spark.table(table_name)
        df_temp.cache()
        df_temp.write.mode('overwrite').format("orc").saveAsTable(table_name + "_temp")
        spark.sql("refresh table " + table_name + "_temp")
        df_temp_ritorno = spark.table(table_name + "_temp")
        df_temp_ritorno.cache()
        df_temp_ritorno.write.mode('overwrite').format("orc").saveAsTable(table_name)
        print('Fine riscrittura in formato ORC-SPARK della tabella ' + table_name)


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

    lista_tabelle = ['loyalty_fca.l_anag',
                     'loyalty_fca.l_balance',
                     'loyalty_fca.l_transaction',
                     'loyalty_fca.p_nfr_cams',
                     'loyalty_fca.p_glf_cams',
                     'loyalty_fca.p_dwhp2_cams',
                     'loyalty_fca.p_transaction_saveback',
                     'loyalty_fca.i_nfr_cams_scarti',
                     'loyalty_fca.i_glf_cams_scarti',
                     'loyalty_fca.i_dwhp2_cams_scarti',
                     'loyalty_fca.i_transaction_saveback_scarti'
                     ]

    for t in lista_tabelle:
        tabella_formato_spark(spark, t)
