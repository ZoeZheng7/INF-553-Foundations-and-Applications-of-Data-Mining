import java.io.{BufferedWriter, File, FileWriter}

import breeze.numerics.{abs, pow}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

object ModelBasedCF {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    if (args.length != 2) {
      println(
        """
          |Usage:
          |cd $SPARK_HOME
          |bin/spark-submit --class ModelBasedCF Xiaoyu_Zheng_hw3.jar <rating file path> <testing file path>
        """.stripMargin)
      System.exit(-1)
    }
    val path = args(0)
    val test = args(1)
//    val path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-20m/ratings.csv"
//    val path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-latest-small/ratings.csv"
//    val test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_20m.csv"
//    val test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_small.csv"
    val conf = new SparkConf()
    conf.setAppName("ModelBasedCF-Scala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(path)
    val header = rdd.first()
    // ([user_id, movie_id, rating, timestamp]) -> all data set (all string)
    val dataAll = rdd.filter(row => row != header).map(x => x.split(","))
    // ((user_id, movie_id), rating) -> all data set (all int)
    val data = dataAll.map(x => ((x(0).toInt, x(1).toInt), x(2).toDouble))
    // ([user_id, movie_id]) -> test data (all string)
    val testRddPre = sc.textFile(test)
    val testHeader = testRddPre.first()
    val testRdd = testRddPre.filter(x => x != testHeader).map(x => x.split(","))
//    for (p <- testRdd.take(10)) {
//      for (e <- p) {
//        println(e)
//      }
//    }
//    for (e <- testHeader) {
//      print(e)
//    }
    // (user_id, movie_id) -> test data (all int)
//    val testKeyPre = testRdd
//    for (p <- testKeyPre.take(10)) {
//      for (e <- p) {
//        println(e)
//      }
//    }
    val testKey = testRdd.map(x => (x(0).toInt, x(1).toInt))
//    for (e <- testKey.take(10)) {
//      println(e)
//    }
    // ((user_id, movie_id), null) -> test data (all int)
    val testData = testKey.map(x => (x, 0.toDouble))
    // ((user_id, movie_id), rating) -> all data excluding test data -> train data
    val trainData = data.subtractByKey(testData)
    val ratings = trainData.map(x => Rating(x._1._1, x._1._2, x._2))

//    for (e <- ratings.take(10)) {
//      println(e)
//    }
    val rank = 5
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)
//    val usersProducts = testKey.map { case Rating(user, product, rate) =>
//      (user, product)
//    }
    def cutPred(x: (Int, Int, Float)): ((Int, Int), Float) = {
      var rate = 0.toFloat
      if (x._3 > 5) {
        rate = 5
      }
      else if (x._3 < 1) {
        rate = 1
      }
      else {
        rate = x._3
      }
      ((x._1, x._2), rate)
    }

    //: RDD[((Int, Int), Float)]
    val preds = model.predict(testKey)
      .map{ case Rating(user, product, rate) => (user, product, rate.toFloat)}
      .map(cutPred)
    println(preds)
    val noPreds = testData.subtractByKey(preds).map(x => (x._1, 3.toFloat))
    val predictions = preds.union(noPreds).sortBy(x => x._1._2).sortBy(x => x._1._1)

    println("test count: ")
    println(testKey.count())
    print("result count: ")
    print(predictions.count())
    val ratesAndPreds = data.join(testData).map(x => (x._1, x._2._1)).join(predictions)
    val diff = ratesAndPreds.map(r => abs(r._2._1 - r._2._2.toDouble))
    val diff01 = diff.filter(x => x >= 0 && x < 1).count()
    val diff12 = diff.filter(x => x >= 1 && x < 2).count()
    val diff23 = diff.filter(x => x >= 2 && x < 3).count()
    val diff34 = diff.filter(x => x >= 3 && x < 4).count()
    val diff4 = diff.filter(x => x >= 4).count()
    val MSE = diff.map(x => pow(x, 2)).mean()
    val RMSE = pow(MSE, 0.5)

    val file = new File("Xiaoyu_Zheng_ModelBasedCF.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val it = predictions.collect().iterator
    while (it.hasNext) {
      var e = it.next()
      //      var size = point.length
      bw.write(e._1._1.toString)
      bw.write(", ")
      bw.write(e._1._2.toString)
      bw.write(", ")
      bw.write(e._2.toString)
      bw.write("\n")
    }
    bw.close()
    println(">=0 and <1: "+diff01.toString)
    println(">=1 and <2: "+diff12.toString)
    println(">=2 and <3: "+diff23.toString)
    println(">=3 and <4: "+diff34.toString)
    println(">=4: "+diff4.toString)
    println("RMSE: "+RMSE.toString)
    val end = System.nanoTime()
    println("Time: "+((end-start)/1e9d).toString+" sec")
  }

}
