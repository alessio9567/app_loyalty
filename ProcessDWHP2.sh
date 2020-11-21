echo 'ProcessDWHP2'
export SPARK_MAJOR_VERSION=2
spark-submit /home/hdfs/lbd/loyaltyFcaBank/app_loyalty/ProcessDWHP2.py &> /home/hdfs/lbd/loyaltyFcaBank/app_loyalty/log_spark.txt