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
import Logger


logger = Logger.getLogger()

def get_df_l_balance(sqlContext):
    df_l_balance_copy = sqlContext.table("loyalty_fca.l_balance")
    df_l_balance_copy.cache()
    df_l_balance_copy.write.mode("overwrite").format("orc").saveAsTable("loyalty_fca.l_balance_temp")
    df_l_balance = sqlContext.table("loyalty_fca.l_balance_temp")
    df_l_balance.cache()
    return df_l_balance

def get_df_l_anag(sqlContext):
    df_l_anag_copy = sqlContext.table("loyalty_fca.l_anag")
    df_l_anag_copy.cache()
    df_l_anag_copy.write.mode("overwrite").format("orc").saveAsTable("loyalty_fca.l_anag_temp")
    df_l_anag = sqlContext.table("loyalty_fca.l_anag_temp")
    df_l_anag.cache()
    return df_l_anag

def get_df_l_transaction(sqlContext):
    df_l_transaction_copy = sqlContext.table("loyalty_fca.l_transaction")
    df_l_transaction_copy.cache()
    df_l_transaction_copy.write.mode("overwrite").format("orc").saveAsTable("loyalty_fca.l_transaction_temp")
    df_l_transaction = sqlContext.table("loyalty_fca.l_transaction_temp")
    df_l_transaction.cache()
    return df_l_transaction


# usare questa funzione solo in FASE DI TEST!!!!
def balance_zeroing(sqlContext, balance_type):
    # possibili valori balance_type sono:
    #        balance_extrabonus
    #        balance_virtualcredit
    df_l_balance = get_df_l_balance(sqlContext)
    df_l_balance = df_l_balance.withColumn(balance_type, lit(0)) 
    logger.info('Scrittura azzeramento balance, colonna  ' + balance_type + ' della tabella loyalty_fca.l_balance\n')
    df_l_balance.write.mode('overwrite').format("orc").saveAsTable('loyalty_fca.l_balance')
    sqlContext.sql("refresh table loyalty_fca.l_balance")



def balance_update(sqlContext, df_transaction_sum, balance_type):
    # possibili valori balance_type sono:
    #        balance_extrabonus
    #        balance_virtualcredit
     # il df df_transaction_sum deve essere group_by product_Company, customer_number, transaction_type   
    df_transaction_sum = df_transaction_sum.select(col('product_company').alias('product_company_tr'), 
                                                   col('customer_number').alias('customer_number_tr'),
                                                   col('transaction_type').alias('transaction_type_tr'),
                                                   col('somma').alias('somma_tr')
                                                   )
    df_l_balance = get_df_l_balance(sqlContext)
    L_BALANCE_ORIGINAL_COLUMN_NAMES = df_l_balance.schema.names
    logger.info("L_BALANCE_ORIGINAL_COLUMN_NAMES " + str(L_BALANCE_ORIGINAL_COLUMN_NAMES))
    df_l_balance_precalc = df_l_balance.join(df_transaction_sum, \
                                                (df_l_balance.product_company == df_transaction_sum.product_company_tr) \
                                                & (df_l_balance.customer_number == df_transaction_sum.customer_number_tr),
                                                how='left') 

    df_l_balance_calc = df_l_balance_precalc.withColumn(balance_type, 
                                                        col(balance_type) + coalesce(df_l_balance_precalc.somma_tr,
                                                        lit(0)))  \
                                            .select(L_BALANCE_ORIGINAL_COLUMN_NAMES)
    logger.info('Scrittura in overwrite tabella loyalty_fca.l_balance')
    #df_l_balance_calc = df_l_balance_calc.fillna({balance_type:'0'})
    df_l_balance_calc.write.mode('overwrite').format("orc").saveAsTable('loyalty_fca.l_balance')
    sqlContext.sql("refresh table loyalty_fca.l_balance")


def balance_customer_zeroing(sqlContext, df_action, balance_type):
    # possibili valori balance_type sono:
    #        balance_extrabonus
    #        balance_virtualcredit
    df_action = df_action.select(col('product_company').alias('product_company_ac'), 
                                 col('customer_number').alias('customer_number_ac'),
                                 col('plastic_status_new').alias('plastic_status_new_ac')
                                 )
    df_l_balance = get_df_l_balance(sqlContext)
    L_BALANCE_ORIGINAL_COLUMN_NAMES = df_l_balance.schema.names
    logger.info("L_BALANCE_ORIGINAL_COLUMN_NAMES " + str(L_BALANCE_ORIGINAL_COLUMN_NAMES))
    df_l_balance_precalc = df_l_balance.join(df_action, \
                                                (df_l_balance.product_company == df_action.product_company_ac) \
                                                & (df_l_balance.customer_number == df_action.customer_number_ac) , \
                                                how='left') 

    df_l_balance_calc = df_l_balance_precalc.withColumn(balance_type, when(col('plastic_status_new_ac').isin('GD','CB','CT'), 0).otherwise(col(balance_type))).select(L_BALANCE_ORIGINAL_COLUMN_NAMES)
    logger.info('Scrittura in overwrite tabella loyalty_fca.l_balance\n')
    df_l_balance_calc = df_l_balance_calc.fillna({balance_type:'0'})
    df_l_balance_calc.write.mode('overwrite').format("orc").saveAsTable('loyalty_fca.l_balance')
    sqlContext.sql("refresh table loyalty_fca.l_balance")


def anag_customer_account_disable(sqlContext, df_action, df_l_anag):
        # possibili valori balance_type sono:
        #        balance_extrabonus
        #        balance_virtualcredit
        L_ANAG_ORIGINAL_COLUMN_NAMES = df_l_anag.schema.names
        logger.info("L_ANAG_ORIGINAL_COLUMN_NAMES " + str(L_ANAG_ORIGINAL_COLUMN_NAMES))
        df_action = df_action.select(col('product_company').alias('product_company_ac'), 
                                     col('customer_number').alias('customer_number_ac'),
                                     col('plastic_status_new').alias('plastic_status_new_ac'),
                                     col('elaboration_ts').alias('elaboration_ts_ac')
                                     )
        df_l_anag_precalc = df_l_anag.join(df_action,
                                           (df_l_anag.product_company == df_action.product_company_ac)\
                                                & (df_l_anag.customer_number == df_action.customer_number_ac),
                                            how='left')
        df_l_anag_calc = df_l_anag_precalc.withColumn('is_customer_number_active',
                                                      when(col('plastic_status_new_ac').isin('GD', 'CB','CT'), False) \
                                                                            .otherwise(col('is_customer_number_active')))   \
                                          .withColumn('is_account_number_active',
                                                        when(col('plastic_status_new_ac').isin('GD', 'CB','CT'), False) \
                                                                            .otherwise(col('is_account_number_active'))) \
                                          .withColumn('customer_status',
                                                        coalesce(col('plastic_status_new_ac'),
                                                        col('customer_status')))  \
                                          .withColumn('account_status',
                                                        coalesce(col('plastic_status_new_ac'),
                                                                                   col('account_status')))  \
                                          .withColumn('elaboration_ts',
                                                        coalesce(col('elaboration_ts_ac'),
                                                        col('elaboration_ts')))  \
                                          .select(L_ANAG_ORIGINAL_COLUMN_NAMES)
                                                
        logger.info('Scrittura in overwrite tabella loyalty_fca.l_anag\n')
        df_l_anag_calc.write.mode('overwrite').format("orc").saveAsTable('loyalty_fca.l_anag')
        sqlContext.sql("refresh table loyalty_fca.l_anag")



def overwrite_i_table(sqlContext, df, i_table):
    logger.info('Scrittura in overwrite tabella ' + i_table)
    df.write.mode('overwrite').format("orc").saveAsTable(i_table)
    sqlContext.sql("refresh table " + i_table)

def append_i_table_scarti(sqlContext, df, i_table_scarti):
    logger.info('Scrittura in append tabella ' + i_table_scarti)
    df.write.mode('append').format("orc").saveAsTable(i_table_scarti)
    sqlContext.sql("refresh table " + i_table_scarti)