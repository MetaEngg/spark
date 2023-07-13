from pyspark.sql import *

from kafka import KafkaProducer

from time import sleep


spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

file = r'C:\\tmp\access_log_20230713-130543.log'

with open(file, errors="ignore", mode='r') as f:
    for line in f:
        print(line)
        producer.send('july13', line.encode('utf-8'))
        sleep(2)
