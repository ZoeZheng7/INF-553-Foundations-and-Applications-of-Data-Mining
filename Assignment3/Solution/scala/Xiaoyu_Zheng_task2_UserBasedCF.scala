import java.io.{BufferedWriter, File, FileWriter}

import breeze.linalg.{max, min, sum}
//import breeze.numerics.pow
import org.apache.spark.{SparkConf, SparkContext}


object UserBasedCF {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
        if (args.length != 2) {
          println(
            """
              |Usage:
              |cd $SPARK_HOME
              |bin/spark-submit --class UserBasedCF Xiaoyu_Zheng_hw3.jar <rating file path> <testing file path>
            """.stripMargin)
          System.exit(-1)
        }
        val path = args(0)
        val test = args(1)
//    val path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-latest-small/ratings.csv"
//    val test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_small.csv"
    val conf = new SparkConf()
    conf.setAppName("UserBasedCF-Scala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(path)
    val header = rdd.first()
    // ([user_id, movie_id, rating, timestamp]) -> all data set (all string)
    val dataAll = rdd.filter(row => row != header).map(x => x.split(","))
    // ((user_id, movie_id), rating) -> all data set (all int)
    val data = dataAll.map(x => ((x(0).toInt, x(1).toInt), x(2).toFloat))
    val testRddPre = sc.textFile(test)
    val testHeader = testRddPre.first()
    val testRdd = testRddPre.filter(x => x != testHeader).map(x => x.split(","))
    val testKey = testRdd.map(x => (x(0).toInt, x(1).toInt))
    // ((user_id, movie_id), null) -> test data (all int)
    val testData = testKey.map(x => (x, 0.toFloat))
    // ((user_id, movie_id), rating) -> all data excluding test data -> train data
    val trainData = data.subtractByKey(testData)

    // hashmap(key:(user_id, movie_id), value: rating)
    val dicTrain = scala.collection.mutable.HashMap.empty[(Int, Int), Float]
    for (e <- trainData.collect()) {
      dicTrain(e._1) = e._2
    }

    // (user_id, rating_avg)
    val userAvg = trainData.map(x => {
      val tmp = scala.collection.mutable.ListBuffer.empty[Float]
      tmp += x._2
      (x._1._1, tmp.toList)
    }).reduceByKey((x, y) => x ++ y).map(x => (x._1, sum(x._2)/x._2.length))

    // hashmap(key:user_id, value: rating)
    val dicAvg = scala.collection.mutable.HashMap.empty[Int, Float]
    for (e <- userAvg.collect()) {
      dicAvg(e._1) = e._2
    }

    // (movie_id, [(user_id, rating)])
    val movieToUser = trainData.map(x => {
      val tmp = scala.collection.mutable.ListBuffer.empty[(Int, Float)]
      tmp += Tuple2(x._1._1, x._2)
      (x._1._2, tmp.toList)
    }).reduceByKey((x, y) => x++y)

    def userRatingComb(x: (Int, List[(Int, Float)])): List[((Int, Int), List[(Float, Float)])] = {
      val tmp = scala.collection.mutable.ListBuffer.empty[((Int, Int), List[(Float, Float)])]
      for (y <- x._2.combinations(2)) {
        val temp = scala.collection.mutable.ListBuffer.empty[(Float, Float)]
        var a = (0, 0.toFloat)
        var b = (0, 0.toFloat)
        if (y(0)._1 > y(1)._1) {
          a = y(1)
          b = y(0)
        }
        else {
          a = y(0)
          b = y(1)
        }
        temp += Tuple2(a._2, b._2)
        tmp += Tuple2((a._1, b._1), temp.toList)
      }
      tmp.toList
    }
    // ((user1_id, user2_id), [(rating1, rating2)])    user1_id < user2_id
    val userToRating = movieToUser.flatMap(userRatingComb).reduceByKey((x, y) => x++y)

//    var ind = 0
//    for (e <- userToRating.collect()) {
//      print(e)
//    }

    def calSimi(x: List[(Float, Float)]): Float = {
      var ratingA = scala.collection.mutable.ListBuffer.empty[Float]
      var ratingB = scala.collection.mutable.ListBuffer.empty[Float]
//      var diff1 = scala.collection.mutable.ListBuffer.empty[Float]
//      var diff2 = scala.collection.mutable.ListBuffer.empty[Float]
      var diff1 = 0.toFloat
      var diff2 = 0.toFloat
      var up = 0.toFloat
//      var downSq1 = scala.collection.mutable.ListBuffer.empty[Float]
//      var downSq2 = scala.collection.mutable.ListBuffer.empty[Float]
      var downSq1 = 0.toFloat
      var downSq2 = 0.toFloat
      for (e <- x) {
        ratingA += e._1
        ratingB += e._2
      }
      val avg1 = sum(ratingA)/ratingA.length
      val avg2 = sum(ratingB)/ratingB.length
      for (i <- ratingA.indices) {
        diff1 = ratingA(i) - avg1
        diff2 = ratingA(i) - avg1
        downSq1 += Math.pow(diff1, 2).toFloat
        downSq2 += Math.pow(diff2, 2).toFloat
        up += diff1 * diff2
      }
      val root1 = Math.pow(downSq1, 0.5)
      val root2 = Math.pow(downSq2, 0.5)
      val down = root1 * root2
      if (up == 0) {
        return 0
      }
      (up/down).toFloat
    }

    // ((user1_id, user2_id), pearson_similarity) user1_id < user2_id
    val userSimi = userToRating.map(x => (x._1, calSimi(x._2)))

    // (activeUser_id, [(user1_id, similarity)])
    val userSimiMatrix = userSimi.flatMap(x => {
      var tmp = scala.collection.mutable.ListBuffer.empty[(Int, List[(Int, Float)])]
      var temp1 = scala.collection.mutable.ListBuffer.empty[(Int, Float)]
      var temp2 = scala.collection.mutable.ListBuffer.empty[(Int, Float)]
      temp1 += Tuple2(x._1._2, x._2)
      temp2 += Tuple2(x._1._1, x._2)
      tmp += Tuple2(x._1._1, temp1.toList)
      tmp += Tuple2(x._1._2, temp2.toList)
      tmp.toList
    }).reduceByKey((x, y) => x ++ y)

    val dicSimi = scala.collection.mutable.HashMap.empty[Int, List[(Int, Float)]]
    for (e <- userSimiMatrix.collect()) {
      dicSimi(e._1) = e._2
    }

    def predict(x: (Int, Int)): ((Int, Int), Float) = {
      val allCom = dicSimi(x._1)
//      allCom.sorted()
      val length = allCom.size
//      var companion = List()
//      var companion = scala.collection.mutable.ListBuffer.empty[(Int, Float)].toList
      var companion = List[(Int, Float)]()
      if (length > 5) {
        companion = allCom.slice(0, 5)
      }
      else {
        companion = allCom
      }
      val avg = dicAvg(x._1)
      var sumi = 0.toFloat
      var down = 0.toFloat
//      if (companion)
      for (e <- companion) {
        val tmp = Tuple2(e._1, x._2)
        if (dicTrain.contains(tmp)) {
          val rate = dicTrain(tmp) - dicAvg(e._1)
          sumi += rate * e._2
          down += Math.abs(e._2)
        }
      }
      var returns = Tuple2(Tuple2(0, 0), 0.toFloat)
      if (down == 0) {
        returns = Tuple2(x, avg)
      }
      else {
        returns = Tuple2(x, (sumi/down+avg).toFloat)
      }
      returns
    }

    def cutPred(x: ((Int, Int), Float)): ((Int, Int), Float) = {
      var rate = 0.toFloat
      if (x._2 > 5) {
        rate = 5
      }
      else if (x._2 < 1) {
        rate = 1
      }
      else {
        rate = x._2
      }
      (x._1, rate)
    }

    val predictions = testKey.map(x => predict(x)).map(cutPred).sortBy(x => x._1._2).sortBy(x => x._1._1)

//    for (e <- result.collect()) {
//      print(result)
//    }

    val ratesAndPreds = data.join(testData).map(x => (x._1, x._2._1)).join(predictions)
    val diff = ratesAndPreds.map(r => Math.abs(r._2._1 - r._2._2))
    val diff01 = diff.filter(x => x >= 0 && x < 1).count()
    val diff12 = diff.filter(x => x >= 1 && x < 2).count()
    val diff23 = diff.filter(x => x >= 2 && x < 3).count()
    val diff34 = diff.filter(x => x >= 3 && x < 4).count()
    val diff4 = diff.filter(x => x >= 4).count()
    val MSE = diff.map(x => Math.pow(x, 2)).mean()
    val RMSE = Math.pow(MSE, 0.5)

    val file = new File("Xiaoyu_Zheng_UserBasedCF.txt")
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
