#if u want to store this data in snowflake try this function
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "apr5").load()
df.printSchema()
ndf=df.selectExpr("CAST(value AS STRING)")
log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

res=ndf.select(regexp_extract('value', log_reg, 1).alias('ip'),
                         regexp_extract('value', log_reg, 4).alias('date'),
                         regexp_extract('value', log_reg, 6).alias('request'),
                         regexp_extract('value', log_reg, 10).alias('referrer'))


SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
    "sfURL": "vqkwubf-es59758.snowflakecomputing.com",
    "sfUser": "ARJUN1234",
    "sfPassword": "Etl@1234",
    "sfDatabase": "avddb",
    "sfSchema": "public",
    "sfWarehouse": "COMPUTE_WH"
}

#res.writeStream.outputMode("append").format("console").start().awaitTermination()
def write2Snowflake(df, batch_id):
    df.write.mode("append").format("snowflake").options(**sfOptions).option("dbtable","kafka_structure_streaming").save()
    pass

res.writeStream \
    .foreachBatch(write2Snowflake) \
    .outputMode("append") \
    .start() \
    .awaitTermination()