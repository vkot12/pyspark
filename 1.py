
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder.appName('huinia').getOrCreate()

df = spark.read.csv("reviews.csv")

df.printSchema()

df2 = spark.read.option("header",True) \
     .csv("reviews.csv")
df2.printSchema()
   


df3 = spark.read.options(header='True', delimiter=',') \
  .csv("reviews.csv")
df3.printSchema()


schema = StructType() \
      .add("Id",IntegerType(),True) \
      .add("ProductId",StringType(),True) \
      .add("UserId",StringType(),True) \
      .add("ProfileName",StringType(),True) \
      .add("HelpfulnessNumerator",IntegerType(),True) \
      .add("HelpfulnessDenominator",IntegerType(),True) \
      .add("Score",IntegerType(),True) \
      .add("Time",IntegerType(),True) \
      .add("Summary",StringType(),True) \
      .add("Text",StringType(),True) \
      
      
df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("reviews.csv")
df_with_schema.printSchema()

df2.select("UserId","ProductId")
df2.select(df.UserId,df.ProductId)
df2.select(df["UserId"],df["ProductId"])

df2.write.option("header",True) \
    .csv("/tmp/spark_output/laba123")
    
rdd=df2.rdd
print(rdd.collect())

val pairs = lines.map(x => (x.split(" ")(0), x))

rdd2=rdd.mapValues(x => (x, 1)).rdd.map(lambda r: r[0]).collect().reduceByKey(lambda a,b: a+b)

rdd3=rdd2.map(lambda x: (x[0], sorted(x[1]), x[2]  ))

rdd4=rdd3.take(10)

