import java.io.{BufferedWriter, File, FileWriter}

import breeze.linalg.min

//import breeze.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object JaccardLSH {
  def main(args: Array[String]): Unit = {
    val begin = System.nanoTime()
    if (args.length != 1) {
      println(
        """
          |Usage:
          |cd $SPARK_HOME
          |bin/spark-submit --class JaccardLSH Xiaoyu_Zheng_hw3.jar <rating file path>
        """.stripMargin)
      System.exit(-1)
    }
    val path = args(0)
//    val path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ratings.csv"
    val conf = new SparkConf()
    conf.setAppName("LSH-Scala")
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
    val result = cand.map(x => jaccard(x)).filter(x => x._3 >= 0.5).sortBy(x => x._2).sortBy(x => x._1)

//    val rr = result.collect()

    val ground = sc.textFile("/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/SimilarMovies.GroundTruth.05.csv")
      .map(x => x.split(",")).map(x => (x(0).toInt, x(1).toInt))

    val rrr = result.map(x => (x._1, x._2))
    val tp = ground.intersection(rrr)
    val ttttt = tp.count()
    val rrrrr = rrr.count()
    val ggggg = ground.count()
    val precision = ttttt.toFloat/rrrrr
    val recall = ttttt.toFloat/ggggg

//    println("rrrrr")
//    println(rrrrr)
//    println("ttttt")
//    println(ttttt)
//    println("ggggg")
//    println(ggggg)

    println("precision:")
    println(precision)
    println("recall:")
    println(recall)

    val file = new File("Xiaoyu_Zheng_SimilarMovies_Jaccard.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val it = result.collect().iterator
    while (it.hasNext) {
      var e = it.next()
//      var size = point.length
      bw.write(e._1.toString)
      bw.write(", ")
      bw.write(e._2.toString)
      bw.write(", ")
      bw.write(e._3.toString)
      bw.write("\n")
    }
    bw.close()
    val end = System.nanoTime()
    println("Elapsed time: ")
    println((end-begin)/1e9d)

  }
}
