from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from loyalty_fca_utility import Logger
from loyalty_fca_utility import EmailManager
import sys

if __name__ == '__main__':

    valore_arg = ''
    logger = Logger.getLogger()
    if len(sys.argv) > 1:
        valore_arg = sys.argv[1]



        logger.info('START RestoreDB.py')

        conf = SparkConf().setAppName("app")
        sc = SparkContext.getOrCreate(conf=conf)
        sqlContext = HiveContext(sc)

        elaboration_ts = str(valore_arg)

        L_ANAG = 'loyalty_fca.l_anag'
        L_BALANCE = 'loyalty_fca.l_balance'
        L_TRANSACTION = 'loyalty_fca.l_transaction'


        BCK_ANAG = "loyalty_fca.l_anag" + "_backup" + elaboration_ts
        BCK_BALANCE = "loyalty_fca.l_balance" + "_backup" + elaboration_ts
        BCK_TRANSACTION = "loyalty_fca.l_transaction" + "_backup" + elaboration_ts

        elaboration_ok = True

        try:
            df_anag = sqlContext.table(L_ANAG)
            df_anag.cache()

            df_balance = sqlContext.table(L_BALANCE)
            df_balance.cache()

            df_transaction = sqlContext.table(L_TRANSACTION)
            df_transaction.cache()

        except Exception as e:
            logger.error("Fallita copia iniziale delle tabelle")
            EmailManager.sendEmailWithAttachment(": restore DB",
                                         "Fallita copia iniziale delle tabelle",
                                         logger.logpath)
            elaboration_ok = False

        if elaboration_ok:
            try:
                df_anag_bck = sqlContext.table(BCK_ANAG)
                df_anag_bck.write.mode("overwrite").format("orc").saveAsTable(L_ANAG)
                logger.info('overwrite ' + BCK_ANAG + ' in ' + L_ANAG)
                df_balance_bck = sqlContext.table(BCK_BALANCE)
                df_balance_bck.write.mode("overwrite").format("orc").saveAsTable(L_BALANCE)
                logger.info('overwrite ' + BCK_BALANCE + ' in ' + L_BALANCE)
                df_transaction_bck = sqlContext.table(BCK_TRANSACTION)
                df_transaction_bck.write.mode("overwrite").format("orc").saveAsTable(L_TRANSACTION)
                logger.info('overwrite ' + BCK_TRANSACTION + ' in ' + L_TRANSACTION)
                n_record_anag_bck = str(df_anag_bck.count())
                n_record_balance_bck = str(df_balance_bck.count())
                n_record_transaction_bck = str(df_transaction_bck.count())

                logger.info("LoyaltyFCABank: restore DB " + " del " + elaboration_ts + "-  Restore eseguito correttamente"
                            + "\nRecord l_anag: " + n_record_anag_bck
                            + "\nRecord l_balance: " + n_record_balance_bck
                            + "\nRecord l_transaction: " + n_record_transaction_bck
                            )
                EmailManager.sendEmailWithAttachment(": restore DB " + " del " + elaboration_ts,
                                             "Restore eseguito correttamente"
                                             + "\nRecord l_anag: " + n_record_anag_bck
                                             + "\nRecord l_balance: " + n_record_balance_bck
                                             + "\nRecord l_transaction: " + n_record_transaction_bck,
                                             logger.logpath
                                             )
            except Exception as e:

                try:
                    df_anag.write.mode("overwrite").format("orc").saveAsTable(L_ANAG)
                    df_balance.write.mode("overwrite").format("orc").saveAsTable(L_BALANCE)
                    df_transaction.write.mode("overwrite").format("orc").saveAsTable(L_TRANSACTION)
                    logger.error('ERRORE RESTORE del ' + elaboration_ts)
                    EmailManager.sendEmailWithAttachment(": restore DB",
                                                         "ERRORE NEL RESTORE del " + elaboration_ts,
                                                         logger.logpath)

                except Exception as e:
                    logger.error('ERRORE RESTORE del ' + elaboration_ts +
                                 ' ATTENZIONE! Fallita la procedura di ripristino delle attuali tabelle!')
                    EmailManager.sendEmailWithAttachment(": restore DB",
                                                         "ERRORE NEL RESTORE del " + elaboration_ts +
                                                            " ATTENZIONE! Fallita la procedura di ripristino delle attuali tabelle!",
                                                         logger.logpath)

    else:
        logger.error("Necessario specificare una data in formato yyyyMMdd come parametro di input al restore")

    logger.info('END RestoreDB.py')