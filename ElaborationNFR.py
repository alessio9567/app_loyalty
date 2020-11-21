#!/usr/bin/env python

from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import datetime
import ElaborationSQLUtility
import FileUtility
import CommonUtility
from time import gmtime, strftime
import sys
import Logger

#if __name__ == '__main__':
def execute():
    esito = False
    
    logger = Logger.getLogger()
    logger.info('START ElaborationNFR.py')
    
    reload(sys)
    sys.setdefaultencoding('utf8')
    
    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)
    CommonUtility.setHDFSproperties(sqlContext)
    
    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())
    
    HDFS_ROOT = CommonUtility.getParameterFromFile('HDFS_ROOT')
    OUTPUT_PATH_ACTION = CommonUtility.getParameterFromFile('OUTPUT_PATH_ACTION')
    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_OUTPUT_PATH_ACTION = CommonUtility.getParameterFromFile('FTP_OUTPUT_PATH_ACTION')
    FIXEDPART_ACTION_WINWINIT_OUTPUT = CommonUtility.getParameterFromFile('FIXEDPART_ACTION_WINWINIT_OUTPUT')
    FTP_INPUT_PATH_NFR = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_NFR')
    FTP_WORKED_PATH_NFR = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_NFR')
    E_TABLE = 'loyalty_fca.e_nfr_cams'
    I_TABLE = 'loyalty_fca.i_nfr_cams'
    I_TABLE_SCARTI = 'loyalty_fca.i_nfr_cams_scarti'
    L_ANAG = 'loyalty_fca.l_anag'
    L_BALANCE = 'loyalty_fca.l_balance'
    
    # Regole elaborazione NFR.
    # Non vengono effettuati controlli di esistenza dell'id transazione nello storico
    # Unico controllo viene effettuato sulla presenza dell'accountNumber nell'anagrafica
    # inserire controllo per fare in modo che venga bloccato il processo se la somma dei parziali non corrisponde con quella del totale per accountNumber?
    
    df_l_anag = ElaborationSQLUtility.get_df_l_anag(sqlContext)
    n_record_anag = df_l_anag.count()
    logger.info('Numero record iniziali tabella l_anag: ' + str(n_record_anag))
    
    df_l_balance = ElaborationSQLUtility.get_df_l_balance(sqlContext)
    n_record_balance = df_l_balance.count()
    logger.info('Numero record iniziali tabella l_balance: ' + str(n_record_balance))
    
    df_e_nfr = sqlContext.table(E_TABLE)
    E_NFR_ORIGINAL_COLUMN_NAMES = df_e_nfr.schema.names
    
    logger.info("E_NFR_ORIGINAL_COLUMN_NAMES " + str(E_NFR_ORIGINAL_COLUMN_NAMES))
    
    df_l_anag_alias = df_l_anag.select(col('customer_number').alias('customer_number_an'), 
                                       col('product_company').alias('product_company_an'),
                                       col('account_number').alias('account_number_an')
                                                     )
    
    df_e_nfr_join = df_e_nfr.join(df_l_anag_alias,
                                        (df_e_nfr.product_company == df_l_anag_alias.product_company_an) \
                                        & (df_e_nfr.customer_number == df_l_anag_alias.customer_number_an) \
                                        & (df_e_nfr.account_number == df_l_anag_alias.account_number_an) ,
                                        how='left_outer')
    
    df_i_nfr_cams_NULL = df_e_nfr_join.filter(df_e_nfr_join.customer_number_an.isNull())  
    df_i_nfr_cams = df_i_nfr_cams_NULL.select(E_NFR_ORIGINAL_COLUMN_NAMES)
    df_i_nfr_cams.cache()                                                   
    
    df_i_nfr_cams_NOTNULL = df_e_nfr_join.filter(df_e_nfr_join.customer_number_an.isNotNull())
    df_i_nfr_cams_scarti = df_i_nfr_cams_NOTNULL.select(E_NFR_ORIGINAL_COLUMN_NAMES)  
    df_i_nfr_cams_scarti.cache()
    
    ElaborationSQLUtility.append_i_table_scarti(sqlContext, df_i_nfr_cams_scarti, I_TABLE_SCARTI)
    ElaborationSQLUtility.overwrite_i_table(sqlContext, df_i_nfr_cams, I_TABLE)
    
    n_record_i_table_scarti = df_i_nfr_cams_scarti.count()
    n_record_i_table = df_i_nfr_cams.count()
    logger.info('N record scartati ' + str(n_record_i_table_scarti) + ' righe dalla tabella ' + I_TABLE_SCARTI)
    logger.info('Da elaborare ' + str(n_record_i_table) + ' righe dalla tabella ' + I_TABLE)
    if n_record_i_table > 0:
        ###############################################
        
        df_i_nfr_clean = df_i_nfr_cams.distinct()
        
        #########################################################
        logger.info('####### scrittura action output per winwinit #######')
        #########################################################
        
        file_name = FIXEDPART_ACTION_WINWINIT_OUTPUT + str(elaboration_ts) + '.csv'
        
        df_i_nfr_clean_winwinit = (df_i_nfr_clean.withColumn('data', df_i_nfr_clean.elaboration_ts[0:8])
                                                 .withColumn('actionid', concat(df_i_nfr_clean.appcode,
                                                                                df_i_nfr_clean.elaboration_ts,
                                                                                df_i_nfr_clean.product_company,
                                                                                df_i_nfr_clean.customer_number ))
                                                 .select('appcode',
                                                     'product_company',
                                                     'customer_number',
                                                     lit('').alias('cup'),
                                                     lit('').alias('livrea'),
                                                     lit('').alias('convenzione'),
                                                     lit('').alias('dealer'),
                                                     lit('').alias('plastic_status'),
                                                     'customer_name',
                                                     'customer_surname',
                                                     'customer_address',
                                                     'customer_city',
                                                     'customer_state',
                                                     'zip',
                                                     'country_cod',
                                                     'date_birth',
                                                     'home_tel',
                                                     'work_tel',
                                                     'taxid_number',
                                                     'city_birth',
                                                     'gender',
                                                     'nationality',
                                                     'email',
                                                     'data',
                                                     'actionid',
                                                     lit('I').alias('action_anagrafica'),
                                                     lit('').alias('action_bonus'),
                                                     lit('').alias('action_extrabonus'),
                                                     'elaboration_ts') )
        
        
        FileUtility.hdfs_write_csv(df=df_i_nfr_clean_winwinit,
                                   header=False, delimiter=';',
                                   path=HDFS_ROOT + OUTPUT_PATH_ACTION,
                                   file_name=file_name,
                                   spark_version=1)
        
        FileUtility.local_write_csv(df=df_i_nfr_clean_winwinit,
                                    delimiter=';',
                                    path=FTP_ROOT + FTP_OUTPUT_PATH_ACTION,
                                    file_name=file_name)
                                
        ###############################################
        
        logger.info('Scrittura in append tabella ' + L_BALANCE)
        df_balance_append = (df_i_nfr_clean.withColumn('elaboration_ts', lit(elaboration_ts[0:14]))
                                    .select(
                                         'customer_company',
                                         'product_company',
                                         'customer_number',
                                         lit(0).alias('balance_extrabonus'),
                                         lit(0).alias('balance_virtualcredit'),
                                         'elaboration_ts'))
        df_balance_append.write.mode('append').format("orc").saveAsTable(L_BALANCE)
        sqlContext.sql("refresh table " + L_BALANCE)
        
        logger.info('Scrittura in append tabella ' + L_ANAG)
        df_i_nfr_clean.write.mode('append').format("orc").saveAsTable(L_ANAG)
        sqlContext.sql("refresh table " + L_ANAG)
        
        df_l_anag_end = ElaborationSQLUtility.get_df_l_anag(sqlContext)
        n_record_anag_end = df_l_anag_end.count()
        logger.info('Numero record finali tabella l_anag: ' + str(n_record_anag_end))
        
        df_l_balance_end = ElaborationSQLUtility.get_df_l_balance(sqlContext)
        n_record_balance_end = df_l_balance_end.count()
        logger.info('Numero record finali tabella l_balance: ' + str(n_record_balance_end))
        
        esito = True
        
        if n_record_anag + n_record_i_table != n_record_anag_end:
            logger.error("ERRORE: conteggio record l_anag errato")
            esito = False
        
        if n_record_balance + n_record_i_table != n_record_balance_end:
            logger.error("ERRORE: conteggio record l_balance errato")
            esito = False
        
    else:
        logger.warning("Tabella " + I_TABLE + " vuota, nessun file prodotto")
        esito = True
        
    logger.info('END ElaborationNFR.py')
        
    return esito
    