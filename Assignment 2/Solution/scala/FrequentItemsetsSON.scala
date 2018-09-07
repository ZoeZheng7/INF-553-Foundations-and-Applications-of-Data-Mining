import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.JavaConverters._

object FrequentItemsetsSON {
  def main(args: Array[String]): Unit = {
    val begin = System.nanoTime()
    if (args.length != 3) {
      println(
        """
          |Usage:
          |cd $SPARK_HOME
          |bin/spark-submit --class Task1 Xiaoyu_Zheng_SON.jar <case number> <csv file path> <support>
        """.stripMargin)
      System.exit(-1)
    }
    val caseNumber = args(0)
    val path = args(1)
    val support = args(2).toInt
//    val caseNumber = "1"
//    val path = "/Users/xiaoyuzheng/spark/spark-2.2.1-bin-hadoop2.7/Small1.csv"
//    val support = 4

    val conf = new SparkConf()
    conf.setAppName("SON-Scala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(path)
    val header = rdd.first()
    val data = rdd.filter(row => row != header)
    var baskets: RDD[(String, Set[Int])] = null
    if (caseNumber == "1") {
      baskets = data.map(x => x.split(","))
                    .filter(line => line.length > 1)
                    .map(x => (x(0), Set(x(1).toInt)))
                    .reduceByKey((x: Set[Int], y: Set[Int]) => x++y)
    }
    else if (caseNumber == "2") {
      baskets = data.map(x => x.split(","))
        .filter(line => line.length > 1)
        .map(x => (x(1), Set(x(0).toInt)))
        .reduceByKey((x: Set[Int], y: Set[Int]) => x++y)
    }
    else {
      println("case number should only be 1 for case 1, or 2 for case 2")
      System.exit(-1)
    }
    val transactions : RDD[Set[Int]] = baskets.values
//    val transactions = transactionsOri.repartition(1)
    val p = transactions.getNumPartitions


    val aprioriRes = transactions.mapPartitions(x => apriori(x, p, support))
                                 .map(x => {
                                   val tmp = scala.collection.mutable.ListBuffer.empty[Int]
                                   for (e <- x._1) {
                                     tmp += e.toInt
                                   }
                                   Tuple2(tmp.toList.sorted, x._2)
                                 })
                                 .reduceByKey((x: Int, y: Int) => x)
                                 .keys
    val phase1res = aprioriRes.collect().toList

    val res = transactions.mapPartitions(x => countVeri(x, phase1res))
                          .reduceByKey((x, y) => x+y)
                          .filter(x => x._2 >= support)
                          .keys
                          .collect()
                          .toList


    def sort1[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted

    val finals = sort1(res).sortBy(_.size)


    val start = path.lastIndexOf("/")
    val name = "Xiaoyu_Zheng_SON_"+path.substring(start+1, path.length-3)+"case"+caseNumber+"-"+support.toString+".txt"
    val file = new File(name)
    val pw = new BufferedWriter(new FileWriter(file))

    var it = finals.iterator
    if (it.hasNext) {
      var point = it.next()
      var size = point.size
      pw.write("(")
      var i = 0
      for (e <- point) {
        if (i != 0) {
          pw.write(", ")
        }
        pw.write(e.toString)
        i += 1
      }
      pw.write(")")
      while (it.hasNext) {
        point = it.next()
        if (point.size > size) {
          pw.write("\n\n")
        }
        else {
          pw.write(", ")
        }
        size = point.size
        i = 0
        pw.write("(")
        for (e <- point) {
          if (i != 0) {
            pw.write(", ")
          }
          pw.write(e.toString)
          i += 1
        }
        pw.write(")")
      }
    }
    pw.close()
    val end = System.nanoTime()
    println("Elapsed time: ")
    println((end-begin)/1e9d)
  }


  def countVeri(iterator: Iterator[Set[Int]], aprioriRes: List[List[Int]]) :Iterator[(List[Int], Int)] = {
    var returns = scala.collection.mutable.ListBuffer.empty[(List[Int], Int)]
    while (iterator.hasNext) {
      var basket = iterator.next()
      for (subset <- aprioriRes) {
        if (subset.toSet.subsetOf(basket)) {
          returns += Tuple2(subset, 1)
        }
      }
    }
    returns.toList.iterator
  }

  def apriori(x: Iterator[Set[Int]], p: Int, support: Int): Iterator[(List[Int], Int)] = {
    var chunk = scala.collection.mutable.ListBuffer.empty[Set[Int]]
    var singleCount = scala.collection.mutable.HashMap.empty[Int, Int]
    var preRes = scala.collection.mutable.ListBuffer.empty[Set[Int]]

    var returns = scala.collection.mutable.ListBuffer.empty[(List[Int], Int)]
    val threshold = support.toFloat/p
    while (x.hasNext) {
      var basket = x.next()
      chunk += basket
      for (elem <- basket) {
        if (singleCount.contains(elem)) {
          if (singleCount(elem) < threshold) {
            singleCount(elem) = singleCount(elem)+1
            if (singleCount(elem) >= threshold) {
              preRes += Set(elem)
              returns += Tuple2(List(elem), 1)
            }
          }
        }
        else {
          singleCount(elem) = 1
          if (singleCount(elem) >= threshold) {
            preRes += Set(elem)
            returns += Tuple2(List(elem), 1)
          }
        }
      }
    }

    val data = chunk.toList
    var size = 1
    var newCand = scala.collection.mutable.ListBuffer.empty[Set[Int]]
    for (i <- preRes.indices) {
      var x = preRes(i)
      for (j <- (i + 1).until(preRes.length)) {
        var y = preRes(j)
        var tmp = x ++ y
        if (!(newCand contains tmp)) {
          newCand += tmp
        }
      }
    }

    while (newCand.nonEmpty) {
      preRes = scala.collection.mutable.ListBuffer.empty[Set[Int]]
      size += 1
      var count = scala.collection.mutable.HashMap.empty[Set[Int], Int]
      for (basket <- data) {
        for (cand <- newCand) {
          if (cand.subsetOf(basket)) {
            if (count.contains(cand)) {
              if (count(cand) < threshold) {
                count(cand) = count(cand)+1
                if (count(cand) >= threshold) {
                  preRes += cand
                  returns += Tuple2(cand.toList, 1)
                }
              }
            }
            else {
              count(cand) = 1
              if (count(cand) >= threshold) {
                preRes += cand
                returns += Tuple2(cand.toList, 1)
              }
            }
          }
        }
      }
      newCand = scala.collection.mutable.ListBuffer.empty[Set[Int]]
      for (i <- preRes.indices) {
        var x = preRes(i)
        for (j <- (i + 1).until(preRes.length)) {
          var y = preRes(j)
          var tmp = x ++ y
          if (!(newCand contains tmp)) {
            if (tmp.size == size+1) {
              var flag = true
              var t = tmp.toSeq.combinations(size)
              for (element <- t) {
                var ele = element.toSet
                if (!preRes.contains(ele)) {
                  flag = false
                }
              }
              if(flag) {
                newCand += tmp
              }
            }
          }
        }
      }
    }
    val res = returns.toList
    res.iterator
  }
}
