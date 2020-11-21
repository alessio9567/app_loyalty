#!/usr/bin/env python

from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import FileUtility
import CommonUtility
import ElaborationSQLUtility
import Logger
from time import gmtime, strftime


# if __name__ == '__main__':
def execute():
    esito = False

    logger = Logger.getLogger()
    logger.info('START ElaborationGLF.py')

    elaboration_ts = strftime("%Y%m%d%H%M%S", gmtime())

    HDFS_ROOT = CommonUtility.getParameterFromFile('HDFS_ROOT')
    OUTPUT_PATH_TRANSACTION = CommonUtility.getParameterFromFile('OUTPUT_PATH_TRANSACTION')
    FTP_ROOT = CommonUtility.getParameterFromFile('FTP_ROOT')
    FTP_OUTPUT_PATH_TRANSACTION = CommonUtility.getParameterFromFile('FTP_OUTPUT_PATH_TRANSACTION')
    FIXEDPART_TRANSACTION_WINWINIT_OUTPUT = CommonUtility.getParameterFromFile('FIXEDPART_TRANSACTION_WINWINIT_OUTPUT')
    FTP_INPUT_PATH_GLF = CommonUtility.getParameterFromFile('FTP_INPUT_PATH_GLF')
    FTP_WORKED_PATH_GLF = CommonUtility.getParameterFromFile('FTP_WORKED_PATH_GLF')
    E_TABLE = 'loyalty_fca.e_glf_cams'
    I_TABLE = 'loyalty_fca.i_glf_cams'
    I_TABLE_SCARTI = 'loyalty_fca.i_glf_cams_scarti'
    L_ANAG = 'loyalty_fca.l_anag'
    L_BALANCE = 'loyalty_fca.l_balance'
    L_TRANSACTION = 'loyalty_fca.l_transaction'
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

    # Regole elaborazione GLF.
    # Non vengono effettuati controlli di esistenza dell'id transazione nello storico
    # Unico controllo viene effettuato sulla presenza dell'accountNumber nell'anagrafica
    # inserire controllo per fare in modo che venga bloccato il processo se la somma dei parziali non corrisponde con quella del totale per accountNumber?

    df_l_anag = ElaborationSQLUtility.get_df_l_anag(spark)
    n_record_anag = df_l_anag.count()
    logger.info('Numero record iniziali tabella l_anag: ' + str(n_record_anag))

    df_l_transaction = ElaborationSQLUtility.get_df_l_transaction(spark)
    n_record_tr = df_l_transaction.count()
    logger.info('Numero record iniziali tabella ' + L_TRANSACTION + ': ' + str(n_record_tr))

    E_TRANSACTION_ORIGINAL_COLUMN_NAMES = df_l_transaction.schema.names

    df_e_glf = spark.table(E_TABLE)
    E_GLF_ORIGINAL_COLUMN_NAMES = df_e_glf.schema.names

    logger.info("E_GLF_ORIGINAL_COLUMN_NAMES " + str(E_GLF_ORIGINAL_COLUMN_NAMES))

    df_l_anag_alias = df_l_anag.select(col('customer_number').alias('customer_number_an'),
                                       col('product_company').alias('product_company_an'),
                                       col('is_customer_number_active').alias('is_customer_number_active_an'),
                                       col('is_account_number_active').alias('is_account_number_active_an'),
                                       col('account_number').alias('account_number_an')
                                       )

    df_e_glf_join = df_e_glf.join(df_l_anag_alias, \
                                  (df_e_glf.product_company == df_l_anag_alias.product_company_an) \
                                  & (df_e_glf.account_number == df_l_anag_alias.account_number_an), \
                                  how='left_outer')

    df_i_glf_cams_NOTNULL = df_e_glf_join.filter(df_e_glf_join.account_number_an.isNotNull())

    df_i_glf_cams_VALID = df_i_glf_cams_NOTNULL.filter(df_i_glf_cams_NOTNULL.is_customer_number_active_an == True)
    df_i_glf_cams_NOTVALID = df_i_glf_cams_NOTNULL.filter(df_i_glf_cams_NOTNULL.is_customer_number_active_an == False)

    df_i_glf_cams_NULL = df_e_glf_join.filter(df_e_glf_join.account_number_an.isNull())

    df_i_glf_cams = df_i_glf_cams_VALID.select(E_GLF_ORIGINAL_COLUMN_NAMES)
    E_GLF_ORIGINAL_COLUMN_NAMES_customer_number = list(E_GLF_ORIGINAL_COLUMN_NAMES)
    E_GLF_ORIGINAL_COLUMN_NAMES_customer_number.extend(['customer_number'])
    df_i_glf_cams_customer = df_i_glf_cams_VALID.withColumn('customer_number', df_i_glf_cams_VALID.customer_number_an) \
        .select(E_GLF_ORIGINAL_COLUMN_NAMES_customer_number)
    df_i_glf_cams_customer.cache()
    df_i_glf_cams.cache()
    df_i_glf_cams_scarti = df_i_glf_cams_NULL.unionAll(df_i_glf_cams_NOTVALID) \
        .select(E_GLF_ORIGINAL_COLUMN_NAMES)
    df_i_glf_cams_scarti.cache()

    ElaborationSQLUtility.append_i_table_scarti(spark, df_i_glf_cams_scarti, I_TABLE_SCARTI)
    ElaborationSQLUtility.overwrite_i_table(spark, df_i_glf_cams, I_TABLE)

    n_record_i_table_scarti = df_i_glf_cams_scarti.count()
    n_record_i_table = df_i_glf_cams_customer.count()
    logger.info('N record scartati ' + str(n_record_i_table_scarti) + ' righe dalla tabella ' + I_TABLE_SCARTI)
    logger.info('Da elaborare ' + str(n_record_i_table) + ' righe dalla tabella ' + I_TABLE + " con customer")

    if n_record_i_table > 0:

        df_i_glf_cams_new_trans_id = df_i_glf_cams_customer.withColumn('transaction_id',
                                                                       concat(df_i_glf_cams_customer.appcode,
                                                                              df_i_glf_cams_customer.transaction_id,
                                                                              lit('_'),
                                                                              df_i_glf_cams_customer.transaction_date,
                                                                              lit('_'),
                                                                              df_i_glf_cams_customer.input_filename_timestamp))

        df_i_glf_cams_new_trans_id.cache()

        n_record_transazioni = df_i_glf_cams_new_trans_id.count()
        logger.info('Numero record da aggiungere a transaction: ' + str(n_record_transazioni))

        ###############################################
        logger.info('####### scrittura tabella l_balance ###########')
        ###############################################

        df_i_glf_cams_sum = df_i_glf_cams_new_trans_id.groupBy('product_company', 'customer_number', 'transaction_type') \
            .agg(sum(concat("sign", "value")).alias('somma'))

        df_i_glf_cams_sum_vc = df_i_glf_cams_sum.filter(df_i_glf_cams_sum.transaction_type == 'VC')
        df_i_glf_cams_sum_eb = df_i_glf_cams_sum.filter(df_i_glf_cams_sum.transaction_type == 'E')

        n_record_sum_vc = df_i_glf_cams_sum_vc.count()
        n_record_sum_eb = df_i_glf_cams_sum_eb.count()

        if n_record_sum_vc > 0:
            logger.info('Numero account number a cui viene aggiornato balance nei Virtual Credit: ' + str(n_record_sum_vc))
            ElaborationSQLUtility.balance_update(spark, df_i_glf_cams_sum_vc, 'balance_virtualcredit')

        if n_record_sum_eb > 0:
            logger.info('Numero account number a cui viene aggiornato balance negli Extrabonus: ' + str(n_record_sum_eb))
            ElaborationSQLUtility.balance_update(spark, df_i_glf_cams_sum_eb, 'balance_extrabonus')

        ######################################
        ######################################
        logger.info('####### scrittura transaction output per winwinit #######')
        #########################################################

        file_name = FIXEDPART_TRANSACTION_WINWINIT_OUTPUT + str(elaboration_ts) + '.csv'

        df_i_glf_cams_winwinit = (df_i_glf_cams_new_trans_id.select('appcode',
                                                                    'product_company',
                                                                    'customer_number',
                                                                    'transaction_id',
                                                                    'transaction_date',
                                                                    'transaction_value',
                                                                    'transaction_type',
                                                                    'sign',
                                                                    'currency',
                                                                    'value',
                                                                    'causal',
                                                                    'causalcode',
                                                                    'elaboration_ts'))

        FileUtility.hdfs_write_csv(df=df_i_glf_cams_winwinit,
                                   header=False,
                                   delimiter=';',
                                   path=HDFS_ROOT + OUTPUT_PATH_TRANSACTION,
                                   file_name=file_name,
                                   spark_version=2)

        FileUtility.local_write_csv(df=df_i_glf_cams_winwinit,
                                    delimiter=';',
                                    path=FTP_ROOT + FTP_OUTPUT_PATH_TRANSACTION,
                                    file_name=file_name)

        df_i_glf_cams_to_l_transaction = df_i_glf_cams_new_trans_id.select(E_TRANSACTION_ORIGINAL_COLUMN_NAMES)
        logger.info('Scrittura in append tabella ' + L_TRANSACTION)
        df_i_glf_cams_to_l_transaction.write.format("orc").insertInto(L_TRANSACTION)
        spark.sql("refresh table " + L_TRANSACTION)

        df_l_transaction_end = ElaborationSQLUtility.get_df_l_transaction(spark)
        n_record_tr_end = df_l_transaction_end.count()
        logger.info('Numero record finali tabella ' + L_TRANSACTION + ': ' + str(n_record_tr_end))

        esito = True
        if n_record_tr + n_record_transazioni != n_record_tr_end:
            logger.error("Numero finale di record nella " + L_TRANSACTION + " sbagliato!")
            esito = False

    else:
        logger.warning("Tabella " + I_TABLE + " vuota, nessun file prodotto")
        esito = True

    logger.info('END ElaborationGLF.py')

    return esito
