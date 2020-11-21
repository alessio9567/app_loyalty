
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import glob
import os
import subprocess
import Logger

logger = Logger.getLogger()

def is_duplicate_file( df, column, comparison_value ):
    duplicate_file = False
    values_list = df.select(column).rdd.flatMap(lambda x: x).collect()
    if comparison_value in values_list :
        duplicate_file = True
        logger.warning("++++ FILE GIA ELABORATO! " + comparison_value + " duplicato! ++++\n")

    return duplicate_file


def custom_read_csv(sc, schema, header, mode, delimiter, path, spark_version):
    
    fileCsv = ''
    if spark_version == 1:
        logger.info('Lettura CSV in Spark1')
        fileCsv = sc.textFile(path).map(lambda line: line.encode('utf-8').split(delimiter)).filter(lambda line: len(line)>1).collect()
        #.map(lambda line: (line[0],line[1])) 
        #fileCsvScarti = sc.textFile(path).map(lambda line: line.encode('utf-8').split(delimiter)).filter(lambda line: len(line)<=1).collect()
    elif spark_version == 2:
        logger.info('Lettura CSV in Spark2')
        #fileCsv = (sc.read
        #    .schema(schema)
        #    .option("header", header)
        #    .option("mode", mode)
        #    .option("delimiter", delimiter)
        #    .csv(path))
    else:
        logger.error('Versione Spark non disponibile')
    
    return fileCsv


def hdfs_write_csv(df, header, delimiter, path, file_name, spark_version):
    
    logger.info('Scrittura del file HDFS ' + path + file_name + '\n')
    fileCsv = ''
    if spark_version == 1:
        logger.info('Scrittura CSV in Spark1')
        df.rdd.map(lambda x: delimiter.join(map(unicode, x))).coalesce(1).saveAsTextFile(path + file_name)

    elif spark_version == 2:
        logger.info('Scrittura CSV in Spark2')
        df.write.csv(path + file_name)
    else:
        logger.error('Versione Spark non disponibile')


def local_write_csv(df, delimiter, path, file_name):
    
        logger.info('Scrittura nel file nella cartella ftp ' + path + file_name + '\n')
        # scrittura nella cartella ftp
        ofile  = open(path + file_name, "wb")
        #writer = csv.writer(ofile, delimiter=';', quotechar='"')
        for row in df.rdd.map(lambda x: delimiter.join(map(unicode, x))).collect():
            #print ("riga del CSV ---------->   " + str(row.encode('utf-8')))
            ofile.write(str(row.encode('utf-8')) + '\n')
                        
        ofile.close()




class InputFile(object):
    input_filename = ""
    input_filename_timestamp = ""
    input_filename_AbsPath = ""

# The class "constructor" - It's actually an initializer 
    def __init__(self, input_filename, input_filename_timestamp, input_filename_AbsPath):
        self.input_filename = input_filename
        self.input_filename_timestamp = input_filename_timestamp
        self.input_filename_AbsPath = input_filename_AbsPath



def getInputFilename(FILEPATH, FIXEDPART, LEN_TIMESTAMP):
    logger.info("Parametro in input FILEPATH : " + FILEPATH)
    logger.info("Parametro in input FIXEDPART : " + FIXEDPART)
    fileList = sorted(glob.glob(os.path.join(FILEPATH, FIXEDPART + '*')))
    
    input_filename_AbsPath = ''
    input_filename = ''
    input_filename_timestamp = ''

    if len(fileList) > 0:
        logger.info("Nome del primo file : " + fileList[0])
        input_filename_AbsPath = fileList[0]
        input_filename = os.path.basename(input_filename_AbsPath)

        i_start = len(FIXEDPART) 
        i_end = len(FIXEDPART) + int(LEN_TIMESTAMP)
        input_filename_timestamp = input_filename[i_start:i_end]

    inputFile = InputFile(input_filename, input_filename_timestamp, input_filename_AbsPath)
    return inputFile


def getHDFSInputFilename(FILEPATH):
    args = "hdfs dfs -ls "+ FILEPATH +" | awk '{print $8}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    fileList = s_output.split()
    input_filename_AbsPath = 'hdfs://' + fileList[0]
    input_filename = os.path.basename(input_filename_AbsPath)
    input_filename_timestamp = input_filename[7:24]

    inputFile = InputFile(input_filename, input_filename_timestamp, input_filename_AbsPath)
    return inputFile



