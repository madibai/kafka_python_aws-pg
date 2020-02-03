import sys
import os
import shutil

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils
import json

outputPath = '/tmp/spark/checkpoint_01'


# -------------------------------------------------
# Getting the SQL Template
# -------------------------------------------------
def get_sql_query():
    strSQL = 'select ' \
             'from_unixtime(unix_timestamp()) as curr_time,'\
             '    t.city                        as city,' \
             '    t.currency                      as currency,' \
             '    sum(amount)                     as amount ' \
             'from exchanges_stream t ' \
             'group by' \
             '    t.city,' \
             '    t.currency'
    return strSQL


# -------------------------------------------------
# Lazily instantiated global instance of SparkSession
# -------------------------------------------------
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


# -------------------------------------------------
# What I want to do per each RDD...
# -------------------------------------------------
def process(time, rdd):
    print("===========-----> %s <-----===========" % str(time))

    try:
        spark = getSparkSessionInstance(rdd.context.getConf())

        rowRdd = rdd.map(lambda w: Row(city=w['city'],
                                       currency=w['currency'],
                                       amount=w['amount']))

        testDataFrame = spark.createDataFrame(rowRdd)

        testDataFrame.createOrReplaceTempView("exchanges_stream")

        sql_query = get_sql_query()
        testResultDataFrame = spark.sql(sql_query)
        testResultDataFrame.show(n=5)

        # Insert into DB
        try:
            testResultDataFrame.write \
                .format("jdbc") \
                .mode("append") \
                .option("driver", 'org.postgresql.Driver') \
                .option("url", "jdbc:postgresql://dbtest-1.cmwfjvlei66t.eu-central-1.rds.amazonaws.com:5432/") \
                .option("dbtable", "exchanges") \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .save()
            print('DB write succesfull !')
        except Exception as e:
            print("-->Errrorrr with DB working!", e)

    except Exception as e:
        print("--> Error!!!", e)


# -------------------------------------------------
# General function
# -------------------------------------------------
def createContext():
    sc = SparkContext(appName="PythonStreamingKafkaTransaction")
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 10)#  2

    broker_list, topic = sys.argv[1:]

    try:
        directKafkaStream = KafkaUtils.createDirectStream(ssc,
                                                          [topic],
                                                          {"metadata.broker.list": broker_list})
    except:
        raise ConnectionError("Kafka error: Connection refused: \
                            broker_list={} topic={}".format(broker_list, topic))

    parsed_lines = directKafkaStream.map(lambda v: json.loads(v[1]))

    # RDD handling
    parsed_lines.foreachRDD(process)

    return ssc


# -------------------------------------------------
# Begin
# -------------------------------------------------
if __name__ == "__main__":

    if len(sys.argv) != 3:
        #  print("Usage: spark_job.py <zk> <topic>", file=sys.stderr)
        exit(-1)

print("--> Creating new context")
if os.path.exists(outputPath):
    shutil.rmtree('outputPath')

ssc = StreamingContext.getOrCreate(outputPath, lambda: createContext())
ssc.start()
ssc.awaitTermination()
