
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Task1 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |Usage:
          |cd $SPARK_HOME
          |bin/spark-submit --class Task1 Xiaoyu_Zheng_task1.jar <input path> <output path>
        """.stripMargin)
      System.exit(-1)
    }

    val path = args(0)
    val output = args(1)

    //    val sc = new SparkContext()
    //    val sqlContext = new SQLContext(sc)

    val spark = SparkSession.builder.appName("task1").getOrCreate()
    val sc = spark.sparkContext
    //    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    //    val sqlContext = new SQLContext(sc)
    val data = spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(path)

    //    data.show();

    val res = data.groupBy("movieId")
      .agg(sum("rating"), count("timestamp"))
      //      .agg({"rating":"sum", "timestamp":"count"})
      .withColumnRenamed("sum(rating)", "total")
      .withColumnRenamed("count(timestamp)", "count")
      .selectExpr("movieId", "total/count as rating_avg")
      .orderBy(asc("movieId"))

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


