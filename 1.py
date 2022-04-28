
import pyspark
from pyspark.sql import SparkSession
import os

isProd = True

number_cores = 2
memory_gb = 4

conf = (
    SparkConf()
        .setAppName("mandarin")
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.driver.memory', '{}g'.format(memory_gb))
)

if isProd:
    if not os.path.exists('input/Reviews.csv'):
        sc.stop()
        raise Exception("""
            Download the 'Reviews.csv' file from https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews
            and put it in 'input' folder
        """)
    else:
        inputRdd = sc.textFile("input/Reviews.csv")
else:
    inputRdd = sc.textFile("input/Sample.csv")
    
filteredInput = inputRdd.filter(lambda line: line.startswith("Id,") == False)
   
print(inputRdd.collect())

val pairs = lines.map(x => (x.split(" ")(0), x))

rdd2=inputRdd.mapValues(x => (x, 1)).inputRdd.map(lambda r: r[0]).collect().reduceByKey(lambda a,b: a+b)

rdd3=rdd2.map(lambda x: (x[0], sorted(x[1]), x[2]  ))

rdd4=rdd3.take(10)

sc.stop()