
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
import MainframeFieldsUtility
import ElaborationSQLUtility
import CommonUtility
import Logger
from time import gmtime, strftime
import sys


def execute():
    esito = False
    
    
    logger = Logger.getLogger()
    logger.info('START ElaborationDWHP2.py')
    
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
    FTP_INPUT_PATH_DWHP2 = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_DWHP2')
    FTP_WORKED_PATH_DWHP2 = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_DWHP2')
    E_TABLE = 'loyalty_fca.e_dwhp2_cams'
    I_TABLE = 'loyalty_fca.i_dwhp2_cams'
    I_TABLE_SCARTI = 'loyalty_fca.i_dwhp2_cams_scarti'
    L_ANAG = 'loyalty_fca.l_anag'
    L_BALANCE = 'loyalty_fca.l_balance'
    
    
    df_l_anag = ElaborationSQLUtility.get_df_l_anag(sqlContext)
    n_record_anag = df_l_anag.count()
    logger.info('Numero record iniziali tabella l_anag: ' + str(n_record_anag))
    
    df_l_balance = ElaborationSQLUtility.get_df_l_balance(sqlContext)
    n_record_balance = df_l_balance.count()
    logger.info('Numero record iniziali tabella l_balance: ' + str(n_record_balance))
    
    
    df_e_dwhp2 = sqlContext.table(E_TABLE)
    E_DWHP2_ORIGINAL_COLUMN_NAMES = df_e_dwhp2.schema.names
    
    logger.info("E_DWHP2_ORIGINAL_COLUMN_NAMES " + str(E_DWHP2_ORIGINAL_COLUMN_NAMES))
    
    df_l_anag_alias = df_l_anag.select(col('customer_number').alias('customer_number_an'), 
                                       col('product_company').alias('product_company_an'),
                                       col('account_number').alias('account_number_an'),
                                       col('customer_name').alias('customer_name_an'),
                                       col('customer_surname').alias('customer_surname_an'),
                                       col('taxid_number'))
    
    df_e_dwhp2_join = df_e_dwhp2.join(df_l_anag_alias,
                                      (df_e_dwhp2.product_company == df_l_anag_alias.product_company_an)
                                            & (df_e_dwhp2.account_number == df_l_anag_alias.account_number_an),
                                      how='left_outer')
    
    df_i_dwhp2_cams_NOTNULL = df_e_dwhp2_join.filter(df_e_dwhp2_join.account_number_an.isNotNull())    
    df_i_dwhp2_cams = df_i_dwhp2_cams_NOTNULL.select(col('appcode'),
                                                     col('customer_number_an').alias('customer_number'),
                                                     col('product_company_an').alias('product_company'),
                                                     col('account_number_an').alias('account_number'),
                                                     col('customer_status'),
                                                     col('account_status'),
                                                     col('input_filename'),
                                                     col('input_filename_timestamp'),
                                                     col('elaboration_ts'),
                                                     col('internallog_ts'),
                                                     col('plastic_status_old'),
                                                     col('plastic_status_new'),
                                                     col('customer_name_an'),
                                                     col('customer_surname_an'),
                                                     col('cup_old'),
                                                     col('cup_new'),
                                                     col('livrea_old'),
                                                     col('livrea_new'),
                                                     col('dealer_old'),
                                                     col('dealer_new'),
                                                     col('convenzione_old'),
                                                     col('convenzione_new'),
                                                     col('taxid_number'))
    df_i_dwhp2_cams.cache()                                                   
    
    df_i_dwhp2_cams_NULL = df_e_dwhp2_join.filter(df_e_dwhp2_join.account_number_an.isNull())
    df_i_dwhp2_cams_scarti = df_i_dwhp2_cams_NULL.withColumn('customer_number', df_i_dwhp2_cams_NULL.customer_number_an) \
                                                 .select(E_DWHP2_ORIGINAL_COLUMN_NAMES)
    df_i_dwhp2_cams_scarti.cache()
    
    ElaborationSQLUtility.append_i_table_scarti(sqlContext, df_i_dwhp2_cams_scarti, I_TABLE_SCARTI)
    ElaborationSQLUtility.overwrite_i_table(sqlContext, df_i_dwhp2_cams, I_TABLE)
    
    n_record_i_table_scarti = df_i_dwhp2_cams_scarti.count()
    n_record_i_table = df_i_dwhp2_cams.count()
    logger.info('N record scartati ' + str(n_record_i_table_scarti) + ' righe dalla tabella ' + I_TABLE_SCARTI)
    logger.info('Da elaborare ' + str(n_record_i_table) + ' righe dalla tabella ' + I_TABLE)
    
    if n_record_i_table > 0:
        
        df_i_dwhp2_clean = df_i_dwhp2_cams.distinct()
        df_i_dwhp2_clean.cache()
    
        #########################################################                                          
        logger.info('####### scrittura action output per winwinit #######')                                          
        #########################################################
    
        file_name = FIXEDPART_ACTION_WINWINIT_OUTPUT + str(elaboration_ts) + '.csv'
    
        df_i_dwhp2_clean_winwinit = (df_i_dwhp2_clean.withColumn('data', df_i_dwhp2_clean.elaboration_ts[0:8])
         .withColumn('actionid', concat(df_i_dwhp2_clean.appcode,
        df_i_dwhp2_clean.elaboration_ts,
        df_i_dwhp2_clean.product_company,
        df_i_dwhp2_clean.customer_number ))
         .select('appcode',
         'product_company',
         'customer_number',
         'cup_old',
         'cup_new',
         'livrea_old',
         'livrea_new',
         'convenzione_old',
         'convenzione_new',
         'dealer_old',
         'dealer_new',
         'plastic_status_old',
         'plastic_status_new',
         'customer_name_an',
         'customer_surname_an',
         lit('').alias('customer_address'),
         lit('').alias('customer_city'),
         lit('').alias('customer_state'),
         lit('').alias('zip'),
         lit('').alias('country_cod'),
         lit('').alias('date_birth'),
         lit('').alias('home_tel'),
         lit('').alias('work_tel'),
         'taxid_number',
         lit('').alias('city_birth'),
         lit('').alias('gender'),
         lit('').alias('nationality'),
         lit('').alias('email'),
         'data',
         'actionid',
         lit('').alias('action_anagrafica'),
         lit('').alias('action_bonus'),
         lit('').alias('action_extrabonus'),
         'elaboration_ts'))
        
        df_i_dwhp2_clean_winwinit1 = df_i_dwhp2_clean_winwinit.where(df_i_dwhp2_clean_winwinit.plastic_status_new == 'KK').withColumn('action_anagrafica',lit('I')).withColumn('action_bonus',lit('')).withColumn('action_extrabonus',lit('B'))
        
        df_i_dwhp2_clean_winwinit2 = df_i_dwhp2_clean_winwinit.where(df_i_dwhp2_clean_winwinit.plastic_status_new.isin('CB','CT','GD')).withColumn('action_anagrafica',lit('D')).withColumn('action_bonus',lit('Z')).withColumn('action_extrabonus',lit('Z'))
        
        df_i_dwhp2_clean_winwinit3 = df_i_dwhp2_clean_winwinit.where((df_i_dwhp2_clean_winwinit.plastic_status_new.isin('KD','JD')) | ((df_i_dwhp2_clean_winwinit.plastic_status_old == 'AA') & (df_i_dwhp2_clean_winwinit.plastic_status_new == 'DD'))).withColumn('action_anagrafica',lit('U')).withColumn('action_bonus',lit('')).withColumn('action_extrabonus',lit('B'))
        
        df_i_dwhp2_clean_winwinit4 = df_i_dwhp2_clean_winwinit.where((df_i_dwhp2_clean_winwinit.plastic_status_old == '') & (df_i_dwhp2_clean_winwinit.plastic_status_new == 'AA')).withColumn('action_anagrafica',lit('I')).withColumn('action_bonus',lit('')).withColumn('action_extrabonus',lit(''))
        
        df_i_dwhp2_clean_winwinit5 = df_i_dwhp2_clean_winwinit.where((df_i_dwhp2_clean_winwinit.plastic_status_old.isin ('KK','DD')) & (df_i_dwhp2_clean_winwinit.plastic_status_new == 'AA')).withColumn('action_anagrafica',lit('U')).withColumn('action_bonus',lit('')).withColumn('action_extrabonus',lit(''))
        
        df_i_dwhp2_clean_winwinit_tot1 = df_i_dwhp2_clean_winwinit1.unionAll(df_i_dwhp2_clean_winwinit2)
        
        df_i_dwhp2_clean_winwinit_tot2 = df_i_dwhp2_clean_winwinit3.unionAll(df_i_dwhp2_clean_winwinit4)
        
        df_i_dwhp2_clean_winwinit_tot3 = df_i_dwhp2_clean_winwinit_tot1.unionAll(df_i_dwhp2_clean_winwinit_tot2)
        
        df_i_dwhp2_clean_winwinit_tot = df_i_dwhp2_clean_winwinit_tot3.unionAll(df_i_dwhp2_clean_winwinit5)
        
        df_i_dwhp2_clean_winwinit_tot = df_i_dwhp2_clean_winwinit_tot.withColumn('plastic_type', when(col('plastic_status_new').isin('KK'), lit('KK'))
                                                                                                  .when(col('plastic_status_new').isin('AA'), lit('OL')))
        
        df_i_dwhp2_clean_winwinit_tot_plastic_type1 = df_i_dwhp2_clean_winwinit_tot.where(col('plastic_type').isNull() & col('plastic_status_old').isin('AA'))
        
        df_i_dwhp2_clean_winwinit_tot_plastic_type1 = df_i_dwhp2_clean_winwinit_tot_plastic_type1.withColumn('plastic_type', lit('OL'))
        
        df_i_dwhp2_clean_winwinit_tot_plastic_type2 = df_i_dwhp2_clean_winwinit_tot.where(col('plastic_type').isNull() & col('plastic_status_old').isin('KK'))
        
        df_i_dwhp2_clean_winwinit_tot_plastic_type2 = df_i_dwhp2_clean_winwinit_tot_plastic_type2.withColumn('plastic_type', lit('KK'))
        
        df_i_dwhp2_clean_winwinit_tot = df_i_dwhp2_clean_winwinit_tot.filter(df_i_dwhp2_clean_winwinit_tot.plastic_type.isNotNull())
        
        df_i_dwhp2_clean_winwinit_tot = df_i_dwhp2_clean_winwinit_tot.unionAll(df_i_dwhp2_clean_winwinit_tot_plastic_type1)
        
        df_i_dwhp2_clean_winwinit_tot = df_i_dwhp2_clean_winwinit_tot.unionAll(df_i_dwhp2_clean_winwinit_tot_plastic_type2)
        
        df_i_dwhp2_clean_winwinit_tot = df_i_dwhp2_clean_winwinit_tot.select('appcode',
         'product_company',
         'customer_number',
         'cup_new',
         'livrea_new',
         'convenzione_new',
         'dealer_new',
         'plastic_type',
         'customer_name_an',
         'customer_surname_an',
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
         'action_anagrafica',
         'action_bonus',
         'action_extrabonus',
         'elaboration_ts')
        
        df_i_dwhp2_clean_winwinit_tot.cache()
        
        FileUtility.hdfs_write_csv(df = df_i_dwhp2_clean_winwinit_tot,
           header=False,
           delimiter=';',
           path=HDFS_ROOT + OUTPUT_PATH_ACTION,
           file_name=file_name,
           spark_version=1)
        
        FileUtility.local_write_csv(df=df_i_dwhp2_clean_winwinit_tot,
        delimiter=';',
        path=FTP_ROOT + FTP_OUTPUT_PATH_ACTION,
        file_name=file_name)
        
        ###############################################
           
        if df_i_dwhp2_clean_winwinit2.count() > 0:
            logger.info('####### Inizio scrittura tabella l_balance ###########')
            ElaborationSQLUtility.balance_customer_zeroing(sqlContext, df_i_dwhp2_clean_winwinit2, 'balance_virtualcredit')
            ElaborationSQLUtility.balance_customer_zeroing(sqlContext, df_i_dwhp2_clean_winwinit2, 'balance_extrabonus')
        ###############################################
          
        if df_i_dwhp2_clean_winwinit2.count() > 0:
            logger.info('Inizio scrittura in overwrite tabella loyalty_fca.l_anag')
            ElaborationSQLUtility.anag_customer_account_disable(sqlContext, df_i_dwhp2_clean_winwinit2, df_l_anag)
        
        df_l_anag_end = ElaborationSQLUtility.get_df_l_anag(sqlContext)
        n_record_anag_end = df_l_anag_end.count()
        logger.info('Numero record finali tabella l_anag: ' + str(n_record_anag_end))
        
        df_l_balance_end = ElaborationSQLUtility.get_df_l_balance(sqlContext)
        n_record_balance_end = df_l_balance_end.count()
        logger.info('Numero record finali tabella l_balance: ' + str(n_record_balance_end))
        
        esito = True
        
        #if n_record_anag != n_record_anag_end:
        #    logger.error("ERRORE: conteggio record l_anag errato")
        #    esito = False
        
        #if n_record_balance != n_record_balance_end:
        #    logger.error("ERRORE: conteggio record l_balance errato")
        #    esito = False
    
    else:
        logger.warning("Tabella " + I_TABLE + " vuota, nessun file prodotto")
        esito = True
    
    logger.info('END ElaborationDWHP2.py')
    
    return esito