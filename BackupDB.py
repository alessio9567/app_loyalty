from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from time import gmtime, strftime
from loyalty_fca_utility import Logger
from loyalty_fca_utility import EmailManager

if __name__ == '__main__':
    logger = Logger.getLogger()

    logger.info('START BackupDB.py')

    conf = SparkConf().setAppName("app")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = HiveContext(sc)

    elaboration_ts = strftime("%Y%m%d", gmtime())

    L_ANAG = 'loyalty_fca.l_anag'
    L_BALANCE = 'loyalty_fca.l_balance'
    L_TRANSACTION = 'loyalty_fca.l_transaction'
    NEW_ANAG = "loyalty_fca.l_anag" + "_backup" + elaboration_ts
    NEW_BALANCE = "loyalty_fca.l_balance" + "_backup" + elaboration_ts
    NEW_TRANSACTION = "loyalty_fca.l_transaction" + "_backup" + elaboration_ts

    try:
        df_anag = sqlContext.table(L_ANAG)
        n_record_anag = df_anag.count()

        df_balance = sqlContext.table(L_BALANCE)
        n_record_balance = df_balance.count()

        df_transaction = sqlContext.table(L_TRANSACTION)
        n_record_transaction = df_transaction.count()

        if(n_record_anag != 0 and n_record_balance != 0 and n_record_transaction != 0):
            df_anag.write.mode("overwrite").format("orc").saveAsTable(NEW_ANAG)
            df_balance.write.mode("overwrite").format("orc").saveAsTable(NEW_BALANCE)
            df_transaction.write.mode("overwrite").format("orc").saveAsTable(NEW_TRANSACTION)

            logger.info("LoyaltyFCABank: backup DB " + " del " + elaboration_ts + " eseguito correttamente"
                        + "\nRecord l_anag: " + str(n_record_anag)
                        + "\nRecord l_balance: " + str(n_record_balance)
                        + "\nRecord l_transaction: " + str(n_record_transaction)
                        )

            EmailManager.sendEmailWithAttachment(": backup DB",
                                                 "Backup eseguito correttamente"
                                                    + "\nRecord l_anag: " + str(n_record_anag)
                                                    + "\nRecord l_balance: " + str(n_record_balance)
                                                    + "\nRecord l_transaction: " + str(n_record_transaction),
                                                 logger.logpath
                                         )

        else:
            logger.info("LoyaltyFCABank: backup DB " + " del " + elaboration_ts
                        + " non eseguito a causa di tabelle vuote")
            EmailManager.sendEmailWithAttachment(": backup DB",
                                                 "Backup non eseguito a causa di tabelle vuote",
                                                 logger.logpath)
    except Exception as e:
        logger.error('ERRORE CREAZIONE TABELLE DI BACKUP del ' + elaboration_ts)
        EmailManager.sendEmailWithAttachment(": backup DB",
                                             "ERRORE NEL BACKUP del " + elaboration_ts,
                                             logger.logpath)


    logger.info('END BackupDB.py')