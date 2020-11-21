

import pyspark.sql
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
import ElaborationSQLUtility
import RecordTypeGLF
import MainframeFieldsUtility
import Logger
from time import gmtime, strftime


# if __name__ == '__main__':
def execute():
    esito = False
    
    logger = Logger.getLogger()
    logger.info('START ElaborationFCAB.py')
    
    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    CommonUtility.setHDFSproperties(sqlContext)
    
    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())
    
    HDFS_ROOT = CommonUtility.getParameterFromFile('HDFS_ROOT')
    OUTPUT_PATH_TRANSACTION = CommonUtility.getParameterFromFile('OUTPUT_PATH_TRANSACTION')
    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_OUTPUT_PATH_TRANSACTION = CommonUtility.getParameterFromFile('FTP_OUTPUT_PATH_TRANSACTION')
    FTP_RESPONSE_PATH_TRANSACTION_FCAB = CommonUtility.getParameterFromFile('FTP_RESPONSE_PATH_TRANSACTION_FCAB')
    FIXEDPART_TRANSACTION_WINWINIT_OUTPUT = CommonUtility.getParameterFromFile('FIXEDPART_TRANSACTION_WINWINIT_OUTPUT')
    E_TABLE = 'loyalty_fca.e_transaction_fcab'
    I_TABLE = 'loyalty_fca.i_transaction_fcab'
    I_TABLE_SCARTI = 'loyalty_fca.i_transaction_fcab_scarti'
    L_TRANSACTION = 'loyalty_fca.l_transaction'
    
    df_l_anag = ElaborationSQLUtility.get_df_l_anag(sqlContext)
    n_record_anag = df_l_anag.count()
    logger.info('Numero record iniziali tabella l_anag: ' + str(n_record_anag))
    
    df_l_transaction = ElaborationSQLUtility.get_df_l_transaction(sqlContext)
    n_record_tr = df_l_transaction.count()
    logger.info('Numero record iniziali tabella ' + L_TRANSACTION + ': ' + str(n_record_tr))
    
    E_TRANSACTION_ORIGINAL_COLUMN_NAMES = df_l_transaction.schema.names
    
    df_e_transaction_fcab = sqlContext.table(E_TABLE)
    E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES = df_e_transaction_fcab.schema.names
    E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES_2 = list (E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES[0:11])
    E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES_2.extend(['input_filename_timestamp','Cod_Esito_Elab','Desc_Esito_Elab'])
    
    logger.info("E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES " + str(E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES))
    
    df_l_anag_alias = df_l_anag.select(col('customer_number').alias('customer_number_an'),
                                       col('product_company').alias('product_company_an'),
                                       col('is_customer_number_active').alias('is_customer_number_active_an'),
                                       col('is_account_number_active').alias('is_account_number_active_an'),
                                       col('account_number').alias('account_number_an')
                                       )
    
    df_e_transaction_fcab_join = df_e_transaction_fcab.join(df_l_anag_alias, \
                                  (df_e_transaction_fcab.product_company == df_l_anag_alias.product_company_an) \
                                  & (df_e_transaction_fcab.customer_number == df_l_anag_alias.account_number_an), \
                                  how='left_outer')
    
    df_i_transaction_fcab_NOTNULL = df_e_transaction_fcab_join.filter(df_e_transaction_fcab_join.account_number_an.isNotNull())
    
    df_i_transaction_fcab_VALID = df_i_transaction_fcab_NOTNULL.filter(df_i_transaction_fcab_NOTNULL.is_customer_number_active_an == True).\
    withColumn('Cod_Esito_Elab',lit('00')).\
    withColumn('Desc_Esito_Elab',lit('Elaborato Con Successo'))
    
    df_i_transaction_fcab_NULL = df_e_transaction_fcab_join.filter(df_e_transaction_fcab_join.account_number_an.isNull()).\
    withColumn('Cod_Esito_Elab',lit('01')).\
    withColumn('Desc_Esito_Elab',lit('CustomerNumber Inesistente'))
    
    df_e_transaction_fcab_response = df_i_transaction_fcab_VALID.unionAll(df_i_transaction_fcab_NULL)
    
    df_i_transaction_fcab_NOTVALID = df_i_transaction_fcab_NOTNULL.filter(df_i_transaction_fcab_NOTNULL.is_customer_number_active_an == False).\
    withColumn('Cod_Esito_Elab',lit('02')).\
    withColumn('Desc_Esito_Elab',lit('CustomerNumber Non Attivo'))
    
    df_e_transaction_fcab_response = df_e_transaction_fcab_response.unionAll(df_i_transaction_fcab_NOTVALID)
    
    df_i_transaction_fcab_NOTVALIDAPPCODE = df_e_transaction_fcab_join.where( df_e_transaction_fcab_join.appcode != 'CHP' ).\
    withColumn('Cod_Esito_Elab',lit('03')).\
    withColumn('Desc_Esito_Elab',lit('ApplicationCode Non Valido'))
    
    df_e_transaction_fcab_response = df_e_transaction_fcab_response.unionAll(df_i_transaction_fcab_NOTVALIDAPPCODE)
    
    df_i_transaction_fcab_NOTVALIDCLIENTNUMBER = df_e_transaction_fcab_join.where( df_e_transaction_fcab_join.product_company != '18200' ).\
    withColumn('Cod_Esito_Elab',lit('04')).\
    withColumn('Desc_Esito_Elab',lit('ClientNumber Non Valido'))
    
    df_e_transaction_fcab_response = df_e_transaction_fcab_response.unionAll(df_i_transaction_fcab_NOTVALIDCLIENTNUMBER)
    
    df_i_transaction_fcab_NOTVALIDTRANSACTIONTYPE = df_e_transaction_fcab_join.where( df_e_transaction_fcab_join.transaction_type.isin('VC','E') == False ).\
    withColumn('Cod_Esito_Elab',lit('05')).\
    withColumn('Desc_Esito_Elab',lit('Type Non Valido'))
    
    df_e_transaction_fcab_response = df_e_transaction_fcab_response.unionAll(df_i_transaction_fcab_NOTVALIDTRANSACTIONTYPE)
    
    df_i_transaction_fcab_NOTVALIDCASUALCODE = df_e_transaction_fcab_join.where( df_e_transaction_fcab_join.causalcode.isin('BB','DA','PC','PB','CC','WB') == False ).\
    withColumn('Cod_Esito_Elab',lit('06')).\
    withColumn('Desc_Esito_Elab',lit('CausalCode Non Valido'))
    
    df_e_transaction_fcab_response = df_e_transaction_fcab_response.unionAll(df_i_transaction_fcab_NOTVALIDCASUALCODE)
    
    df_i_transaction_fcab= df_i_transaction_fcab_VALID.select(E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES)
    E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES_customer_number = list(E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES)
    E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES_customer_number.extend(['customer_number'])
    df_i_transaction_fcab_customer = df_i_transaction_fcab_VALID.withColumn('customer_number', df_i_transaction_fcab_VALID.customer_number_an) \
        .select(E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES_customer_number)
    df_i_transaction_fcab_customer.cache()
    df_i_transaction_fcab.cache()
    df_i_transaction_fcab_scarti = df_i_transaction_fcab_NULL.unionAll(df_i_transaction_fcab_NOTVALID) \
        .select(E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES)
    df_i_transaction_fcab_scarti.cache()
    
    ElaborationSQLUtility.append_i_table_scarti(sqlContext, df_i_transaction_fcab_scarti, I_TABLE_SCARTI)
    ElaborationSQLUtility.overwrite_i_table(sqlContext, df_i_transaction_fcab, I_TABLE)
    
    n_record_i_table_scarti = df_i_transaction_fcab_scarti.count()
    n_record_i_table = df_i_transaction_fcab_customer.count()
    logger.info('N record scartati ' + str(n_record_i_table_scarti) + ' righe dalla tabella ' + I_TABLE_SCARTI)
    logger.info('Da elaborare ' + str(n_record_i_table) + ' righe dalla tabella ' + I_TABLE + " con customer")
    
    # CREAZIONE FLUSSO DI RESPONSE PER FCAB
    
    df_e_transaction_fcab_response = df_e_transaction_fcab_response.select(E_TRANSACTION_FCAB_ORIGINAL_COLUMN_NAMES_2)
    df_e_transaction_fcab_response = df_e_transaction_fcab_response.orderBy(["customer_number","transaction_id"])
    response_file_name = "CLOUD.80383.LYFCA." + df_e_transaction_fcab_join.select(col("input_filename")).collect()[0][0][:30] + "_SIAResponse.csv"
    
    FileUtility.local_write_csv(df=df_e_transaction_fcab_response,
                                delimiter=';',
                                path = FTP_ROOT+FTP_RESPONSE_PATH_TRANSACTION_FCAB,
                                file_name=response_file_name)
    
    if n_record_i_table > 0:
        
        df_i_transaction_fcab_new_trans_id = df_i_transaction_fcab_customer.withColumn('transaction_id',
                                                                       concat(df_i_transaction_fcab_customer.appcode,
                                                                              df_i_transaction_fcab_customer.transaction_id,
                                                                              lit('_'),
                                                                              df_i_transaction_fcab_customer.transaction_date,
                                                                              lit('_'),
                                                                              df_i_transaction_fcab_customer.input_filename_timestamp))
        
        df_i_transaction_fcab_new_trans_id.cache()
        
        n_record_transazioni = df_i_transaction_fcab_new_trans_id.count()
        logger.info('Numero record da aggiungere a transaction: ' + str(n_record_transazioni))
        
        ###############################################
        logger.info('####### scrittura tabella l_balance ###########')
        ###############################################
        
        df_i_transaction_fcab_sum = df_i_transaction_fcab_new_trans_id.groupBy('product_company','customer_number','transaction_type').agg(sum(concat("sign", "value")).alias('somma'))
        
        #VC E EXTRABONUS IN UNITA'
        df_i_transaction_fcab_sum_vc = df_i_transaction_fcab_sum.filter(df_i_transaction_fcab_sum.transaction_type == 'VC')
        df_i_transaction_fcab_sum_vc=df_i_transaction_fcab_sum_vc.withColumn('somma',col("somma")/1000)
        df_i_transaction_fcab_sum_eb = df_i_transaction_fcab_sum.filter(df_i_transaction_fcab_sum.transaction_type == 'E')
        
        n_record_sum_vc = df_i_transaction_fcab_sum_vc.count()
        n_record_sum_eb = df_i_transaction_fcab_sum_eb.count()
        
        if n_record_sum_vc > 0:
            logger.info('Numero account number a cui viene aggiornato balance nei Virtual Credit: ' + str(n_record_sum_vc))
            ElaborationSQLUtility.balance_update(sqlContext, df_i_transaction_fcab_sum_vc, 'balance_virtualcredit')
        
        if n_record_sum_eb > 0:
            logger.info('Numero account number a cui viene aggiornato balance negli Extrabonus: ' + str(n_record_sum_eb))
            ElaborationSQLUtility.balance_update(sqlContext, df_i_transaction_fcab_sum_eb, 'balance_extrabonus')
        
        ######################################
        ######################################
        logger.info('####### scrittura transaction output per winwinit #######')
        #########################################################
        
        file_name = FIXEDPART_TRANSACTION_WINWINIT_OUTPUT + str(elaboration_ts) + '.csv'
        
        df_i_transaction_fcab_winwinit = df_i_transaction_fcab_new_trans_id.select(col('appcode').alias('appcode'),
                                                                                   col('product_company').alias('product_company'),
                                                                                   col('customer_number').alias('customer_number'),
                                                                                   col('transaction_id').alias('transaction_id'),
                                                                                   col('transaction_date').alias('transaction_date'),
                                                                                   col('value').alias('transaction_value'),
                                                                                   col('transaction_type').alias('transaction_type'),
                                                                                   col('sign').alias('sign'),
                                                                                   col('currency').alias('currency'),
                                                                                   col('value').alias('value'),
                                                                                   col('causal').alias('causal'),
                                                                                   col('causalcode').alias('causalcode'),
                                                                                   col('elaboration_ts').alias('elaboration_ts'))
        
        #TRANSACTION_VALUE:VC in CENTESIMI,E in centesimi
        #VALUE:VC in UNITA',E in centesimi
        
        df_i_transaction_fcab_winwinit_final=df_i_transaction_fcab_winwinit.withColumn('value',when (col("transaction_type")=='VC',(col("value").cast("float")/1000).cast("int")).otherwise((col("value").cast("float")*100).cast("int")))
        
        df_i_transaction_fcab_winwinit_final2=df_i_transaction_fcab_winwinit_final.withColumn('transaction_value',when (col("transaction_type")=='VC',(col("transaction_value").cast("float")/10).cast("int")).otherwise((col("transaction_value").cast("float")*100).cast("int")))
        
        
        FileUtility.hdfs_write_csv(df=df_i_transaction_fcab_winwinit_final2,
                                   header=False,
                                   delimiter=';',
                                   path=HDFS_ROOT+OUTPUT_PATH_TRANSACTION,
                                   file_name=file_name,
                                   spark_version=1)
        
        FileUtility.local_write_csv(df=df_i_transaction_fcab_winwinit_final2,
                                    delimiter=';',
                                    path=FTP_ROOT+FTP_OUTPUT_PATH_TRANSACTION,
                                    file_name=file_name)
        
        
        df_i_transaction_fcab_to_l_transaction=df_i_transaction_fcab_new_trans_id.select(col('appcode').alias('appcode'),
                                                                                         col('product_company').alias('product_company'),
                                                                                         col('customer_number').alias('customer_number'),
                                                                                         col('customer_number').alias('loyalty_program_cod'),
                                                                                         col('transaction_id').alias('transaction_id'),
                                                                                         col('transaction_date').alias('transaction_date'),
                                                                                         col('value').alias('transaction_value'),
                                                                                         col('transaction_type').alias('transaction_type'),
                                                                                         col('sign').alias('sign'),
                                                                                         col('currency').alias('currency'),
                                                                                         col('value').alias('value'),
                                                                                         col('causal').alias('causal'),
                                                                                         col('causalcode').alias('causalcode'),
                                                                                         col('input_filename').alias('input_filename'),
                                                                                         col('input_filename_timestamp').alias('input_filename_timestamp'),
                                                                                         col('elaboration_ts').alias('elaboration_ts'),
                                                                                         col('internallog_ts').alias('internallog_ts'))
        #TRANSACTION_VALUE:VC in CENTESIMI,E in centesimi
        #VALUE:VC in UNITA',E in centesimi
        
        df_i_transaction_fcab_to_l_transaction1=df_i_transaction_fcab_to_l_transaction.withColumn('value',when (col("transaction_type")=='VC',(col("value").cast("float")/1000).cast("int")).otherwise((col("value").cast("float")*100).cast("int")))
        
        df_i_transaction_fcab_to_l_transaction2=df_i_transaction_fcab_to_l_transaction1.withColumn('transaction_value',when (col("transaction_type")=='VC',(col("transaction_value").cast("float")/10).cast("int")).otherwise((col("transaction_value").cast("float")*100).cast("int")))
        
        df_i_transaction_fcab_to_l_transaction3=df_i_transaction_fcab_to_l_transaction2.withColumn('loyalty_program_cod',lit(0))
        
        logger.info('Scrittura in append tabella ' + L_TRANSACTION)
        df_i_transaction_fcab_to_l_transaction3.write.mode('append').format("orc").saveAsTable(L_TRANSACTION)
        sqlContext.sql("refresh table " + L_TRANSACTION)
        
        df_l_transaction_end = ElaborationSQLUtility.get_df_l_transaction(sqlContext)
        n_record_tr_end = df_l_transaction_end.count()
        logger.info('Numero record finali tabella ' + L_TRANSACTION + ': ' + str(n_record_tr_end))
        
        esito = True
        if n_record_tr + n_record_transazioni != n_record_tr_end:
            logger.error("Numero finale di record nella " + L_TRANSACTION + " sbagliato!")
            esito = False
        
    else:
        logger.warning("Tabella " + I_TABLE + " vuota, nessun file prodotto")
        esito = True
        
    logger.info('END ElaborationFCAB.py')
    
    return esito
