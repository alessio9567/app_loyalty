echo 'ProcessTransactionFCAB'
export SPARK_MAJOR_VERSION=2
spark-submit  /home/hdfs/lbd/loyaltyFcaBank/app_loyalty/ProcessFCABTransaction.py &> /home/hdfs/lbd/loyaltyFcaBank/app_loyalty/log_spark.txt
