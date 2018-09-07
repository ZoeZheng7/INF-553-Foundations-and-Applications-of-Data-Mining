
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Task2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println(
        """
          |Usage:
          |cd $SPARK_HOME
          |bin/spark-submit --class Task1 Xiaoyu_Zheng_task1.jar <rating path> <tag path> <output path>
        """.stripMargin)
      System.exit(-1)
    }
    val ratePath = args(0)
    val tagPath = args(1)
    val output = args(2)

    val spark = SparkSession.builder.appName("task1").getOrCreate()
    val sc = spark.sparkContext
    val rate = spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(ratePath)
    val tag = spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(tagPath)

    val movieRate = rate.groupBy("movieId")
      .agg(sum("rating"), count("timestamp"))
      .withColumnRenamed("sum(rating)", "total")
      .withColumnRenamed("count(timestamp)", "count")
      .select("movieId", "total", "count")

    val res = tag
      .join(movieRate, Seq("movieId"))
      .groupBy("tag")
      .agg(sum("total"), sum("count"))
      .withColumnRenamed("sum(total)", "allRate")
      .withColumnRenamed("sum(count)", "allCount")
      .selectExpr("tag", "allRate/allCount as rating_avg")
      .orderBy(desc("tag"))

    //    movieRate.show()
    //    res.show()

    res
      .repartition(1)
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(output)

    spark.stop()

  }

}
