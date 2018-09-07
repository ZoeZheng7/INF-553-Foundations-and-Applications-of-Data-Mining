import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.{SparkConf, SparkContext}

object Betweenness {

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    if (args.length != 1) {
      println(
        """
          |Usage:
          |cd $SPARK_HOME
          |bin/spark-submit --class Betweenness Xiaoyu_Zheng_hw4.jar <rating file path>
        """.stripMargin)
      System.exit(-1)
    }
    val path = args(0)
//    val path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_04/data/ratings.csv"
    val conf = new SparkConf()
    conf.setAppName("Betweenness-Scala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(path)
    val header = rdd.first()
    // ([user_id, movie_id, rating, timestamp]) -> all data set (all string)
    val dataAll = rdd.filter(row => row != header).map(x => x.split(","))
    // ((user_id, movie_id), rating) -> all data set (all int)
    val data = dataAll.map(x => (x(0).toInt, x(1).toInt, x(2).toDouble))
    // (movie_id, [(user_id, rating)])
//    val movieToUser = data.map{ case (user, movie, rating) => (movie, [Tuple2(user, rating)])}
    val movieToUser = data.map(x => {
      val tmp = scala.collection.mutable.ListBuffer.empty[(Int, Double)]
      tmp += Tuple2(x._1, x._3)
      (x._2, tmp.toList)
    }).reduceByKey((x, y) => x++y)

    def userRatingComb(x: (Int, List[(Int, Double)])): List[((Int, Int), List[(Double, Double)])] = {
      val tmp = scala.collection.mutable.ListBuffer.empty[((Int, Int), List[(Double, Double)])]
      for (y <- x._2.combinations(2)) {
        val temp = scala.collection.mutable.ListBuffer.empty[(Double, Double)]
        var a = (0, 0.toDouble)
        var b = (0, 0.toDouble)
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

    // (user1_id, user2_id) for all edges
    val edges = userToRating.filter(x => x._2.length >= 9).map(x => x._1)

//    (user1_id, [other_user_id])
    val edgeUsers = edges.flatMap(x => {
      val tmp = scala.collection.mutable.ListBuffer.empty[(Int, List[Int])]
      val tmp1 = scala.collection.mutable.ListBuffer.empty[Int]
      val tmp2 = scala.collection.mutable.ListBuffer.empty[Int]
      tmp1 += x._1
      tmp2 += x._2
      tmp += Tuple2(x._2, tmp1.toList)
      tmp += Tuple2(x._1, tmp2.toList)
      tmp
    }).reduceByKey((x, y) => x++y)

    val dicTree = scala.collection.mutable.HashMap.empty[Int, List[Int]]
    for (e <- edgeUsers.collect()) {
      dicTree(e._1) = e._2
    }

//    println(dicTree(137).sorted)

    def GN(root: Int): List[((Int, Int), Double)] = {
      var head = root
      var bfsTraversal = scala.collection.mutable.ListBuffer.empty[Int]
      bfsTraversal.append(head)
      var nodes = scala.collection.mutable.Set[Int]()
      nodes += head
      val treeLevel = scala.collection.mutable.HashMap.empty[Int, Int]
      treeLevel(head) = 0
      val dicParent = scala.collection.mutable.HashMap.empty[Int, scala.collection.mutable.ListBuffer[Int]]
      var index = 0
      while (index < bfsTraversal.length) {
        head = bfsTraversal(index)
//        println("index")
//        println(index)
//        println("head")
//        println(head)
        val children = dicTree(head)
        for (child <- children) {
//          println("child")
//          println(child)
          if (!nodes.contains(child)) {
//            println("notContains")
            bfsTraversal += child
            nodes += child
            treeLevel(child) = treeLevel(head) + 1
            val childrenList = scala.collection.mutable.ListBuffer.empty[Int]
            childrenList += head
            dicParent(child) = childrenList
//            println("bfsTraversal")
//            println(bfsTraversal)
          }
          else {
//            println("Contains")
            if (treeLevel(child) == treeLevel(head) + 1) {
              dicParent(child).append(head)
            }
          }
        }
        index += 1
      }
      //      println(bfsTraversal)
      //      println(nodes)
      //      println(treeLevel)
      //      println(dicParent)
      val dicNodeWeight = scala.collection.mutable.HashMap.empty[Int, Double]
      val res = scala.collection.mutable.ListBuffer.empty[((Int, Int), Double)]
//      println("length")
//      println(bfsTraversal.length)
      for (i <- (bfsTraversal.length-1) to 0 by -1) {
//        println(i)
        val node = bfsTraversal(i)
        if (!dicNodeWeight.contains(node)) {
          dicNodeWeight(node) = 1
        }
        if (dicParent.contains(node)) {
          val parents = dicParent(node)
          val parentsSize = parents.length
          for (parent <- parents) {
            if (!dicNodeWeight.contains(parent)) {
              dicNodeWeight(parent) = 1
            }
            val addition = dicNodeWeight(node)/parentsSize*1.0
            dicNodeWeight(parent) += addition
            val tmpEdge = Tuple2(Math.min(node, parent), Math.max(node, parent))
            res.append(Tuple2(tmpEdge, addition/2))
          }
        }
      }
      res.toList
    }

//    ((user1_id, user2_id), betweenness)
    val betweenness = edgeUsers.flatMap(x => GN(x._1)).reduceByKey((x, y) => x+y).sortBy(x => x._1._2).sortBy(x => x._1._1)

//    val betweennessIter = betweenness.collect().iterator
//    var size = 0
//    while (betweennessIter.hasNext && size < 10) {
//      val e = betweennessIter.next()
//      println(e)
//      size += 1
//    }

//    println(betweenness.take(10))
    val file = new File("Xiaoyu_Zheng_Betweenness.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val it = betweenness.collect().iterator
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

    val end = System.nanoTime()
    println("Time: "+((end-start)/1e9d).toString+" sec")

  }

}
