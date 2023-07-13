from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "july13").load()
#df.printSchema()
ndf=df.selectExpr("CAST(value AS STRING)")
# parsed_df = df \
#     .select(df.value.cast("string")).alias("id") \
#     .select(df.value.cast("string")).alias("date") \
#     .select(df.value.cast("string")).alias("request") \
#     .select(df.value.cast("string")).alias("referrer")\
#     .select("id", "date", "request","referrer")
log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'


logs_df =ndf.select(regexp_extract('value', log_reg, 1).alias('ip'),
                         regexp_extract('value', log_reg, 4).alias('date'),
                         regexp_extract('value', log_reg, 6).alias('request'),
                         regexp_extract('value', log_reg, 10).alias('referrer'))


myconf = {
    "url":"jdbc:mysql://mysqldba.cqzgycybrmlz.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false",
    "user":"myuser",
    "password":"12345678",
    "driver":"com.mysql.cj.jdbc.Driver"
}
def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.write.mode("append").format("jdbc").options(**myconf).option("dbtable","kafka_web_logs_structure_streaming").save()
    pass

logs_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()

#logs_df.writeStream.outputMode("append").format("console").start().awaitTermination()