from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("""Usage:
                 cd $SPARK_HOME
                 bin/spark-submit Xiaoyu_Zheng_task1.py <input path> <output path>  
              """)
        exit(-1)

    path = sys.argv[1]
    output = sys.argv[2]

    spark = SparkSession \
        .builder \
        .appName("task1") \
        .getOrCreate()

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    data = spark.read.csv(path, header=True, inferSchema=True)

    # data.show()
    # data.printSchema()
    #
    # res = data\
    #     .select(data["movieId"], data["rating"])\
    #     .groupBy("movieId") \
    #     .sum()\
    #     .orderBy("movieId", ascending=[True]) \

    # res = data.withColumn("movieId", data["movieId"])\
    #     .withColumn("average", data.agg(F.avg(data.rating)).collect())

    res = data.groupBy("movieId")\
              .agg({"rating":"sum", "timestamp":"count"}) \
              .withColumnRenamed("sum(rating)", "total")\
              .withColumnRenamed("count(timestamp)", "count")\
              .select("movieId", expr("total/count as rating_avg")) \
              .orderBy("movieId", ascending=[True])

# .sort("movieId", ascending=True)


    # res.show()

    res\
      .repartition(1)\
      .write\
      .mode("overwrite") \
      .format("com.databricks.spark.csv")\
      .option("header", "true")\
      .save(output)



    # display(
    #     data.
    #         select(data["movieId"], )
    # )

    spark.stop()
