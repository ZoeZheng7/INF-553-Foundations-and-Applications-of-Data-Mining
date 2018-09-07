from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""cd $SPARK_HOME
                 bin/spark-submit Xiaoyu_Zheng_task2.py <rating path> <tag path> <output path>
              """)
        exit(-1)

    ratePath = sys.argv[1]
    tagPath = sys.argv[2]
    output = sys.argv[3]

    spark = SparkSession\
                        .builder\
                        .appName("task2")\
                        .getOrCreate()

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    rate = spark.read.csv(ratePath, header=True, inferSchema=True)
    tag = spark.read.csv(tagPath, header=True, inferSchema=True)

    # movieRate = rate.groupBy("movieId") \
    #     .agg({"rating":"sum", "timestamp":"count"}) \
    #     .withColumnRenamed("sum(rating)", "total") \
    #     .withColumnRenamed("count(timestamp)", "count") \
    #     .select("movieId", expr("total/count as rating_avg")) \
    #     .orderBy("movieId", ascending=[True])

    movieRate = rate.groupBy("movieId") \
    .agg({"rating":"sum", "timestamp":"count"}) \
    .withColumnRenamed("sum(rating)", "total") \
    .withColumnRenamed("count(timestamp)", "count") \
    .select("movieId", "total", "count")

    res = tag\
        .join(movieRate, tag.movieId == movieRate.movieId)\
        .groupBy("tag")\
        .agg({"total": "sum", "count": "sum"})\
        .withColumnRenamed("sum(total)", "allRate")\
        .withColumnRenamed("sum(count)", "allCount")\
        .select("tag", expr("allRate/allCount as rating_avg"))\
        .orderBy("tag", ascending=[False])

    # res.show()

    res \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .save(output)

    spark.stop()

# TODO: note to write the description: 1. output a directory rather than a single file;
# TODO: 2. output path includes the name of the directory;
# TODO: 3. overwrite the directory with the same name
