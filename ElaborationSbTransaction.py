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
import RecordTypeGLF
import ElaborationSQLUtility
import Logger
import MainframeFieldsUtility
from time import gmtime, strftime


#if __name__ == '__main__':
def execute():
    esito = False
    
    logger = Logger.getLogger()
    logger.info('START ElaborationSbTransaction.py')


    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    CommonUtility.setHDFSproperties(sqlContext)

    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())

    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_INPUT_PATH_TRANSACTION_WINWINIT = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_TRANSACTION_WINWINIT')
    FTP_WORKED_PATH_TRANSACTION_WINWINIT = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_TRANSACTION_WINWINIT')
    E_TABLE = 'loyalty_fca.e_transaction_saveback'
    I_TABLE = 'loyalty_fca.i_transaction_saveback'
    I_TABLE_SCARTI = 'loyalty_fca.i_transaction_saveback_scarti'
    L_ANAG = 'loyalty_fca.l_anag'
    L_TRANSACTION = 'loyalty_fca.l_transaction'
    L_BALANCE = 'loyalty_fca.l_balance'


##############################
    df_l_anag = ElaborationSQLUtility.get_df_l_anag(sqlContext)

    n_record_anag = df_l_anag.count()
    logger.info('Numero record iniziali tabella l_anag: ' + str(n_record_anag))

    df_l_transaction = ElaborationSQLUtility.get_df_l_transaction(sqlContext)

    n_record_tr = df_l_transaction.count()
    logger.info('Numero record iniziali tabella l_transaction: ' + str(n_record_tr))

    df_e_tr_sb = sqlContext.table(E_TABLE)
    E_TRSB_ORIGINAL_COLUMN_NAMES = df_e_tr_sb.schema.names
    
    E_TRANSACTION_ORIGINAL_COLUMN_NAMES = df_l_transaction.schema.names
    
    logger.info("E_TRSB_ORIGINAL_COLUMN_NAMES " + str(E_TRSB_ORIGINAL_COLUMN_NAMES))
    
    df_l_anag_alias = df_l_anag.select(col('customer_number').alias('customer_number_an'), 
                                       col('product_company').alias('product_company_an'),
                                       col('is_customer_number_active').alias('is_customer_number_active_an'),
                                       col('is_account_number_active').alias('is_account_number_active_an'),
                                       col('account_number').alias('account_number_an')
                                        )
    
    df_l_transaction_alias = df_l_transaction.select(col('customer_number').alias('customer_number_tr'), 
                                       col('product_company').alias('product_company_tr'),
                                       col('transaction_id').alias('transaction_id_tr')
                                        )

    df_e_tr_sb_join = df_e_tr_sb.join(df_l_anag_alias, \
                                                (df_e_tr_sb.product_company == df_l_anag_alias.product_company_an) \
                                                & (df_e_tr_sb.customer_number == df_l_anag_alias.customer_number_an) , \
                                                how='left_outer') 
    
    df_i_tr_sb_NOTNULL = df_e_tr_sb_join.filter(df_e_tr_sb_join.customer_number_an.isNotNull())
    
    df_i_tr_sb_VALID = df_i_tr_sb_NOTNULL.filter(df_i_tr_sb_NOTNULL.is_customer_number_active_an == True)
    df_i_tr_sb_NOTVALID = df_i_tr_sb_NOTNULL.filter(df_i_tr_sb_NOTNULL.is_customer_number_active_an == False)
    
    df_i_tr_sb_NULL = df_e_tr_sb_join.filter(df_e_tr_sb_join.customer_number_an.isNull())

    df_i_tr_sb_VALID_join = df_i_tr_sb_VALID.join(df_l_transaction_alias, \
                                                (df_i_tr_sb_VALID.product_company == df_l_transaction_alias.product_company_tr) \
                                                & (df_i_tr_sb_VALID.customer_number == df_l_transaction_alias.customer_number_tr) \
                                                & (df_i_tr_sb_VALID.transaction_id == df_l_transaction_alias.transaction_id_tr) , \
                                                how='left_outer')    
    df_i_tr_sb_VALID_NOTNULL = df_i_tr_sb_VALID_join.filter(df_i_tr_sb_VALID_join.transaction_id_tr.isNotNull())
    df_i_tr_sb_VALID_NULL = df_i_tr_sb_VALID_join.filter(df_i_tr_sb_VALID_join.transaction_id_tr.isNull())

    df_i_transaction_saveback = df_i_tr_sb_VALID_NULL.select(E_TRSB_ORIGINAL_COLUMN_NAMES)
    df_i_transaction_saveback.cache()                                                   

    df_i_tr_sb_VALID_NOTNULL_columns = df_i_tr_sb_VALID_NOTNULL.select(E_TRSB_ORIGINAL_COLUMN_NAMES)
    
    df_i_tr_sb_scarti_pre = df_i_tr_sb_NULL.unionAll(df_i_tr_sb_NOTVALID) \
                                             .select(E_TRSB_ORIGINAL_COLUMN_NAMES)
                                             
    df_i_transaction_saveback_scarti =  df_i_tr_sb_scarti_pre.unionAll(df_i_tr_sb_VALID_NOTNULL_columns)

    ElaborationSQLUtility.append_i_table_scarti(sqlContext, df_i_transaction_saveback_scarti, I_TABLE_SCARTI)
    ElaborationSQLUtility.overwrite_i_table(sqlContext, df_i_transaction_saveback, I_TABLE)

    n_record_i_table_scarti = df_i_transaction_saveback_scarti.count()
    n_record_i_table = df_i_transaction_saveback.count()
    logger.info('N record scartati ' + str(n_record_i_table_scarti) + ' righe dalla tabella ' + I_TABLE_SCARTI)
    logger.info('Da elaborare ' + str(n_record_i_table) + ' righe dalla tabella ' + I_TABLE)

    if n_record_i_table > 0:

        df_i_sb_cams_new_trans_id = df_i_transaction_saveback.withColumn('transaction_id', 
                                                                            concat(df_i_transaction_saveback.appcode, 
                                                                                   df_i_transaction_saveback.transaction_id, 
                                                                                   lit('_'), 
                                                                                   df_i_transaction_saveback.transaction_date, 
                                                                                   lit('_'), 
                                                                                   df_i_transaction_saveback.input_filename_timestamp)) 

        df_i_sb_cams_new_trans_id.cache()

        n_record_transazioni = df_i_sb_cams_new_trans_id.count()
        logger.info('Numero record da aggiungere a transaction: ' + str(n_record_transazioni))

        ###############################################
        logger.info('####### scrittura tabella l_balance ###########')                                           
        ###############################################
    
        df_i_sb_sum = df_i_sb_cams_new_trans_id.groupBy('product_company', 'customer_number', 'transaction_type') \
                                               .agg(sum(concat("sign", "value")).alias('somma'))

        df_i_sb_sum_vc = df_i_sb_sum.filter(df_i_sb_sum.transaction_type == 'VC')
        df_i_sb_sum_eb = df_i_sb_sum.filter(df_i_sb_sum.transaction_type == 'E')

        n_record_sum_vc = df_i_sb_sum_vc.count()
        n_record_sum_eb = df_i_sb_sum_eb.count()
               
        if n_record_sum_vc > 0:
            logger.info('Numero account number a cui viene aggiornato balance nei Virtual Credit: ' + str(n_record_sum_vc))
            ElaborationSQLUtility.balance_update(sqlContext, df_i_sb_sum_vc, 'balance_virtualcredit')
   
        if n_record_sum_eb > 0:
            logger.info('Numero account number a cui viene aggiornato balance negli Extrabonus: ' + str(n_record_sum_eb))
            ElaborationSQLUtility.balance_update(sqlContext, df_i_sb_sum_eb, 'balance_extrabonus')
        #######################################
        ###############################################
        logger.info('Scrittura in append tabella ' + L_TRANSACTION)
        df_i_sb_cams_new_trans_id.withColumn('loyalty_program_cod', lit('')) \
                                 .select(E_TRANSACTION_ORIGINAL_COLUMN_NAMES) \
                                 .write.mode('append').format("orc").saveAsTable(L_TRANSACTION)
        sqlContext.sql("refresh table " + L_TRANSACTION)

        df_l_transaction_end = ElaborationSQLUtility.get_df_l_transaction(sqlContext)
        n_record_tr_end = df_l_transaction_end.count()
        logger.info('Numero record finali tabella l_transaction: ' + str(n_record_tr_end))

        esito = True

        if n_record_tr + n_record_transazioni != n_record_tr_end:
            logger.error("Numero finale di record nella transaction sbagliato!")
            esito = False

    else:
        logger.warning("Tabella " + I_TABLE + " vuota, nessun file prodotto")
        esito = True

    logger.info('END ElaborationSbTransaction.py')

    return esito