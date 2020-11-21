from pyspark.sql import SparkSession
from loyalty_fca_utility import CommonUtility
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':

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

    table_name = 'loyalty_fca.p_glf_cams'
    print('Inizio migrazione della tabella: ' + table_name)
    df_temp = spark.table(table_name).select('appcode',
                                                'product_company',
                                                'account_number',
                                                'loyalty_program_cod',
                                                'transaction_id',
                                                'transaction_date',
                                                lit(0).alias('transaction_value'),
                                                'transaction_type',
                                                'sign',
                                                'currency',
                                                'value',
                                                'causal',
                                                'causalcode',
                                                'input_filename',
                                                'input_filename_timestamp',
                                                'elaboration_ts',
                                                'internallog_ts')
    df_temp.cache()
    df_temp.write.mode('overwrite').format("orc").saveAsTable(table_name + "_migrazione")
    spark.sql("refresh table " + table_name + "_migrazione")
    df_temp_ritorno = spark.table(table_name + "_migrazione")
    df_temp_ritorno.cache()
    df_temp_ritorno.write.mode('overwrite').format("orc").saveAsTable(table_name)
    print('Fine migrazione della tabella: ' + table_name)

    table_name = 'loyalty_fca.i_glf_cams_scarti'
    print('Inizio migrazione della tabella: ' + table_name)
    df_temp = spark.table(table_name).select('appcode',
                                                'product_company',
                                                'account_number',
                                                'loyalty_program_cod',
                                                'transaction_id',
                                                'transaction_date',
                                                lit(0).alias('transaction_value'),
                                                'transaction_type',
                                                'sign',
                                                'currency',
                                                'value',
                                                'causal',
                                                'causalcode',
                                                'input_filename',
                                                'input_filename_timestamp',
                                                'elaboration_ts',
                                                'internallog_ts')
    df_temp.cache()
    df_temp.write.mode('overwrite').format("orc").saveAsTable(table_name + "_migrazione")
    spark.sql("refresh table " + table_name + "_migrazione")
    df_temp_ritorno = spark.table(table_name + "_migrazione")
    df_temp_ritorno.cache()
    df_temp_ritorno.write.mode('overwrite').format("orc").saveAsTable(table_name)
    print('Fine migrazione della tabella: ' + table_name)


    table_name = 'loyalty_fca.p_transaction_saveback'
    print('Inizio migrazione della tabella: ' + table_name)
    df_temp = spark.table(table_name).select('appcode',
                                                'product_company',
                                                'customer_number',
                                                'transaction_id',
                                                'transaction_date',
                                                lit(0).alias('transaction_value'),
                                                'transaction_type',
                                                'sign',
                                                'currency',
                                                'value',
                                                'causal',
                                                'causalcode',
                                                'input_filename',
                                                'input_filename_timestamp',
                                                'elaboration_ts',
                                                'internallog_ts')
    df_temp.cache()
    df_temp.write.mode('overwrite').format("orc").saveAsTable(table_name + "_migrazione")
    spark.sql("refresh table " + table_name + "_migrazione")
    df_temp_ritorno = spark.table(table_name + "_migrazione")
    df_temp_ritorno.cache()
    df_temp_ritorno.write.mode('overwrite').format("orc").saveAsTable(table_name)
    print('Fine migrazione della tabella: ' + table_name)

    table_name = 'loyalty_fca.i_transaction_saveback_scarti'
    print('Inizio migrazione della tabella: ' + table_name)
    df_temp = spark.table(table_name).select('appcode',
                                                'product_company',
                                                'customer_number',
                                                'transaction_id',
                                                'transaction_date',
                                                lit(0).alias('transaction_value'),
                                                'transaction_type',
                                                'sign',
                                                'currency',
                                                'value',
                                                'causal',
                                                'causalcode',
                                                'input_filename',
                                                'input_filename_timestamp',
                                                'elaboration_ts',
                                                'internallog_ts')
    df_temp.cache()
    df_temp.write.mode('overwrite').format("orc").saveAsTable(table_name + "_migrazione")
    spark.sql("refresh table " + table_name + "_migrazione")
    df_temp_ritorno = spark.table(table_name + "_migrazione")
    df_temp_ritorno.cache()
    df_temp_ritorno.write.mode('overwrite').format("orc").saveAsTable(table_name)
    print('Fine migrazione della tabella: ' + table_name)




    table_name = 'loyalty_fca.l_transaction'
    print('Inizio migrazione della tabella: ' + table_name)
    df_temp = spark.table(table_name).select('appcode',
                                                'product_company',
                                                'customer_number',
                                                'loyalty_program_cod',
                                                'transaction_id',
                                                'transaction_date',
                                                lit(0).alias('transaction_value'),
                                                'transaction_type',
                                                'sign',
                                                'currency',
                                                'value',
                                                'causal',
                                                'causalcode',
                                                'input_filename',
                                                'input_filename_timestamp',
                                                'elaboration_ts',
                                                'internallog_ts')
    df_temp.cache()
    df_temp.write.mode('overwrite').format("orc").saveAsTable(table_name + "_migrazione")
    spark.sql("refresh table " + table_name + "_migrazione")
    df_temp_ritorno = spark.table(table_name + "_migrazione")
    df_temp_ritorno.cache()
    df_temp_ritorno.write.mode('overwrite').format("orc").saveAsTable(table_name)
    print('Fine migrazione della tabella: ' + table_name)





