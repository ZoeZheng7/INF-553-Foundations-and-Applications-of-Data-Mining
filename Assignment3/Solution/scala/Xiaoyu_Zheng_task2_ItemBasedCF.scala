import java.io.{BufferedWriter, File, FileWriter}

import breeze.linalg.sum
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ItemBasedCF {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
        if (args.length != 2) {
          println(
            """
              |Usage:
              |cd $SPARK_HOME
              |bin/spark-submit --class ItemBasedCF Xiaoyu_Zheng_hw3.jar <rating file path> <testing file path>
            """.stripMargin)
          System.exit(-1)
        }
        val path = args(0)
        val test = args(1)
//    val path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-latest-small/ratings.csv"
//    val test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_small.csv"
    val conf = new SparkConf()
    conf.setAppName("ItemBasedCF-Scala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(path)
    val header = rdd.first()
    val data = rdd.filter(row => row != header).map(x => x.split(","))

    val mov = data.map(x => (x(1).toInt, 1)).reduceByKey((x, y) => x).map(x => x._1)
    //    val mov = movi.map(x => x(0))
    // list of all movie_id
    val movies = mov.collect().toList.sorted
    val dicM = scala.collection.mutable.HashMap.empty[Int, Int]
    var indexOfMovies = 0
    for (e <- movies) {
      dicM(e) = indexOfMovies
      indexOfMovies += 1
    }
    val usr = data.map(x => (x(0).toInt, 1)).reduceByKey((x, y) => x).map(x => x._1)
    //    val mov = movi.map(x => x(0))
    // list of all movie_id
    val users = usr.collect().toList.sorted
    val dicU = scala.collection.mutable.HashMap.empty[Int, Int]
    var indexOfUsers = 0
    for (e <- users) {
      dicU(e) = indexOfUsers
      indexOfUsers += 1
    }
    //    print(dicU)
    val vu = sc.broadcast(dicU)
    val mat = data.map(x => {
      val tmp = scala.collection.mutable.ListBuffer.empty[Int]
      tmp += vu.value(x(0).toInt)
      Tuple2(x(1).toInt, tmp.toList)
    })
      .reduceByKey((x, y) => List.concat(x, y))
      .sortBy(x => x._1)
    val matrix = mat.collect()

    val m = users.length  // m: the number of the bins


    //    print(matrix)
    // hash function
    //    def f(x: Iterator[])
    def f(x: (Int, List[Int]), has: List[Int]): Int = {
      val a = has(0)
      val b = has(1)
      val p = has(2)
      val tmp = scala.collection.mutable.ListBuffer.empty[Int]
      for (e <- x._2) {
        val temp = ((a * e + b) % p) % m
        tmp += temp
      }
      val length = tmp.length
      var min = Int.MaxValue
      if (length > 0) {
        for (e <- tmp) {
          min = Math.min(min, e)
        }
      }
      min
    }

    val hashes = List(List(913, 901, 24593), List(14, 23, 769), List(1, 101, 193),
      List(17, 91, 1543), List(387, 552, 98317), List(11, 37, 3079), List(2, 63, 97),
      List(41, 67, 6151), List(91, 29, 12289), List(3, 79, 53), List(73, 803, 49157),
      List(8, 119, 389))

    // (movie_id, [signature])
    val signatures = mat.map(x => {
      //      var tmp = 0
      val tmp = scala.collection.mutable.ListBuffer.empty[Int]
      for (has <- hashes) {
        tmp += f(x, has)
      }
      Tuple2(x._1, tmp.toList)
    })

    //    val sig = signatures.collect()

    //    for (i <- 0 to 10) {
    //      print(sig(i))
    //    }

    //    print(signatures.collect().take(10))

    val n = hashes.length // 12
    val b = 6
    val r = n/b

    def sig(x: (Int, List[Int])): List[((Int, Tuple1[List[Int]]), List[Int])] = {
      //      val res = scala.collection.mutable.ListBuffer.empty[Tuple2[Tuple2[Int, Tuple1[List[Int]]], List[Int]]]
      val res = scala.collection.mutable.ListBuffer.empty[((Int, Tuple1[List[Int]]), List[Int])]
      for (i <- 0 until b) {
        res += Tuple2(Tuple2(i, Tuple1(x._2.slice(i*r, (i+1)*r))), List(x._1))
      }
      res.toList
    }

    def pairs(x: ((Int, Tuple1[List[Int]]), List[Int])): List[((Int, Int), Int)] = {
      val res = scala.collection.mutable.ListBuffer.empty[((Int, Int), Int)]
      val length = x._2.length
      val whole = x._2.sorted
      for (i <- 0 until length) {
        for (j <- i+1 until length) {
          res += Tuple2(Tuple2(whole(i), whole(j)), 1)
        }
      }
      res.toList
    }

    // [(movie1_id, movie2_id)]
    //    val candPre1: RDD[((Int, Tuple1[List[Int]]), List[Int])] = signatures.flatMap(x => sig(x)).reduceByKey((x, y) => x ++ y)
    //                                                                        .filter(x => x._2.length > 1)
    //    val candPre2: RDD[((Int, Int), Int)] = candPre1.flatMap(x => pairs(x)).reduceByKey((x, y) => x)
    //    val cand : RDD[(Int, Int)] = candPre2.map(x => x._1)
    val cand = signatures.flatMap(x => sig(x)).reduceByKey((x, y) => x ++ y)
      .filter(x => x._2.length > 1).flatMap(x => pairs(x)).reduceByKey((x, y) => x).map(x => x._1)

    def jaccard(x: (Int, Int)): (Int, Int, Float) = {
      val a = matrix(dicM(x._1))._2.toSet
      val b = matrix(dicM(x._2))._2.toSet
      val inter = a.intersect(b)
      val union = a.union(b)
      val jacc = inter.size.toFloat/union.size
      Tuple3(x._1, x._2, jacc)
    }
//    val result = cand.map(x => jaccard(x)).filter(x => x._3 >= 0.5).sortBy(x => x._2).sortBy(x => x._1)
//    println(result.take(10))

    val simiMovie = cand.map(x => jaccard(x)).filter(x => x._3 >= 0.5)
      .map(x => ((Math.min(x._1, x._2), Math.max(x._1, x._2)), x._3))

    val dataAll = data.map(x => ((x(0).toInt, x(1).toInt), x(2).toFloat))
    val testRdd = sc.textFile(test)
    val testHeader = testRdd.first()
    val testKey = testRdd.filter(row => row != testHeader).map(x => x.split(",")).map(x => (x(0).toInt, x(1).toInt))
    val testData = testKey.map(x => (x, 0.toFloat))

    // ((user_id, movie_id), rating)
    val trainData = dataAll.subtractByKey(testData)

    // hashmap(key:(user_id, movie_id), value: rating)
    val dicTrain = scala.collection.mutable.HashMap.empty[(Int, Int), Float]
    for (e <- trainData.collect()) {
      dicTrain(e._1) = e._2
    }

    // (movie_id, rating_avg)
    val movieAvg = trainData.map(x => {
      val tmp = scala.collection.mutable.ListBuffer.empty[Float]
      tmp += x._2
      (x._1._2, tmp.toList)
    }).reduceByKey((x, y) => x ++ y).map(x => (x._1, sum(x._2)/x._2.length))

    // hashmap(key:movie_id, value: rating)
    val dicAvg = scala.collection.mutable.HashMap.empty[Int, Float]
    for (e <- movieAvg.collect()) {
      dicAvg(e._1) = e._2
    }

    // (user_id, [(movie_id, rating)])
    val userToMovie = trainData.map(x => {
      val tmp = scala.collection.mutable.ListBuffer.empty[(Int, Float)]
      tmp += Tuple2(x._1._2, x._2)
      (x._1._1, tmp.toList)
    }).reduceByKey((x, y) => x++y)

    def movieRatingComb(x: (Int, List[(Int, Float)])): List[((Int, Int), List[(Float, Float)])] = {
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
    // ((movie1_id, movie2_id), [(rating1, rating2)])    movie1_id < movie2_id
    val movieToRating = userToMovie.flatMap(movieRatingComb).reduceByKey((x, y) => x++y)

    val simiMovieToRating: RDD[((Int, Int), List[(Float, Float)])] = movieToRating.join(simiMovie).map(x => (x._1, x._2._1.slice(0, -1)))

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

    // ((movie1_id, movie2_id), pearson_similarity) movie1_id < movie2_id
    val movieSimi = simiMovieToRating.map(x => (x._1, calSimi(x._2)))

    // (activeMovie_id, [(movie1_id, similarity)])
    val movieSimiMatrix = movieSimi.flatMap(x => {
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
    for (e <- movieSimiMatrix.collect()) {
      dicSimi(e._1) = e._2
    }

    def predict(x: (Int, Int)): ((Int, Int), Float) = {
      if (!dicSimi.contains(x._2)) {
        if (!dicAvg.contains(x._2)) {
          return (x, 2.5.toFloat)
        }
        return (x, dicAvg(x._2))
      }
      val allCom = dicSimi(x._2)
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
      val avg = 0.toFloat
      var sumi = 0.toFloat
      var down = 0.toFloat
      //      if (companion)
      for (e <- companion) {
        val tmp = Tuple2(x._1, e._1)
        if (dicTrain.contains(tmp)) {
          val rate = dicTrain(tmp)
          sumi += rate * e._2
          down += Math.abs(e._2)
        }
      }
      var returns = Tuple2(Tuple2(0, 0), 0.toFloat)
      if (down == 0) {
        returns = Tuple2(x, dicAvg(x._2))
      }
      else {
        returns = Tuple2(x, sumi/down+avg)
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

    val ratesAndPreds = dataAll.join(testData).map(x => (x._1, x._2._1)).join(predictions)
    val diff = ratesAndPreds.map(r => Math.abs(r._2._1 - r._2._2))
    val diff01 = diff.filter(x => x >= 0 && x < 1).count()
    val diff12 = diff.filter(x => x >= 1 && x < 2).count()
    val diff23 = diff.filter(x => x >= 2 && x < 3).count()
    val diff34 = diff.filter(x => x >= 3 && x < 4).count()
    val diff4 = diff.filter(x => x >= 4).count()
    val MSE = diff.map(x => Math.pow(x, 2)).mean()
    val RMSE = Math.pow(MSE, 0.5)

    val file = new File("Xiaoyu_Zheng_ItemBasedCF.txt")
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
