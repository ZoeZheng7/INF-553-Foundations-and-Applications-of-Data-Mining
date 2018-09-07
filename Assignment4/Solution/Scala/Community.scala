import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._

object Community {

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
        if (args.length != 1) {
          println(
            """
              |Usage:
              |cd $SPARK_HOME
              |bin/spark-submit --class Community Xiaoyu_Zheng_hw4.jar <rating file path>
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
    }).reduceByKey((x, y) => x ++ y)

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
    val userToRating = movieToUser.flatMap(userRatingComb).reduceByKey((x, y) => x ++ y)

    // (user1_id, user2_id) for all edges
    val edges = userToRating.filter(x => x._2.length >= 9).map(x => x._1)

    //    (user1_id, [other_user_id])
    val edgeUsers = edges.flatMap(x => {
      val tmp = scala.collection.mutable.ListBuffer.empty[(Int, scala.collection.mutable.Set[Int])]
      var tmp1 = scala.collection.mutable.Set[Int]()
      var tmp2 = scala.collection.mutable.Set[Int]()
      tmp1 += x._1
      tmp2 += x._2
      tmp += Tuple2(x._2, tmp1)
      tmp += Tuple2(x._1, tmp2)
      tmp
    }).reduceByKey((x, y) => x ++ y)

    val dicTree = scala.collection.mutable.HashMap.empty[Int, scala.collection.mutable.Set[Int]]
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
      for (i <- (bfsTraversal.length - 1) to 0 by -1) {
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
            val addition = dicNodeWeight(node) / parentsSize * 1.0
            dicNodeWeight(parent) += addition
            val tmpEdge = Tuple2(Math.min(node, parent), Math.max(node, parent))
            res.append(Tuple2(tmpEdge, addition / 2))
          }
        }
      }
      res.toList
    }

    //    ((user1_id, user2_id), betweenness)
    val betweenness = edgeUsers.flatMap(x => GN(x._1)).reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending=false)

    val betweennessIter = betweenness.collect().iterator
    var size = 0
    while (betweennessIter.hasNext && size < 10) {
      val e = betweennessIter.next()
      println(e)
      size += 1
    }

    val edgeNumber = edges.count()

//    val nodes = edges.flatMap(x => {
//      case (user1, user2)
//    })

    val nodes = edges.flatMap{case (user1, user2) => List((1, Set(user1)), (1, Set(user2)))}.reduceByKey((x, y) => x++y)
    val nodeTuple = nodes.collect().toList
//    println(nodeTuple)
    val nodeList = nodeTuple.head._2
//    println("nodeList")
//    println(nodeList)

//    val dicK = scala.collection.mutable.HashMap.empty[Int, Double]
    var modularity = 0.0
    val dicModu = scala.collection.mutable.HashMap.empty[(Int, Int), Double]
    for (i <- nodeList) {
      for (j <- nodeList) {
        var A = 0
        if (dicTree(j).contains(i)) {
          A = 1
        }
        val ki = dicTree(i).size
        val kj = dicTree(j).size
        val tmp = (A-0.5*ki*kj/edgeNumber)/(2*edgeNumber)
        dicModu((i, j)) = tmp
        modularity += tmp
      }
    }

    var communities = scala.collection.mutable.ListBuffer.empty[scala.collection.mutable.Set[Int]]
    communities += scala.collection.mutable.Set(nodeList.toArray:_*)
    var maxModu = modularity
    var maxComm = communities.toList
    println(maxModu)

    def dfsCheck(i: Int, j: Int, visited: scala.collection.mutable.Set[Int]): Boolean = {
      visited += i
      val children = dicTree(i)
      for (child <- children) {
        if (child == j) {
          return true
        }
        if (!visited.contains(child)) {
          if (dfsCheck(child, j, visited)) {
            return true
          }
        }
      }
      false
    }

    def dfs(x: Int, visited: scala.collection.mutable.Set[Int]): scala.collection.mutable.Set[Int] = {
      visited += x
      var vis = visited
      val children = dicTree(x)
      for (child <- children) {
        if (!visited.contains(child)) {
          vis = dfs(child, vis)
        }
      }
      vis
    }

    def calcModularity(community: scala.collection.mutable.Set[Int]): Double = {
      var res = 0.0
      for (i <- community) {
        for (j <- community) {
          res += dicModu((i, j))
        }
      }
      res
    }

    val iter = betweenness.collect().iterator
    while (iter.hasNext) {
      val e = iter.next()
      println(e)
      val i = e._1._1
      val j = e._1._2
      dicTree(i) -= j
      dicTree(j) -= i
      if (!dfsCheck(i, j, scala.collection.mutable.Set[Int]())) {
        // remove old community
        var index = -1
//        println
//        var ind = 0
//        var notFound = true
//        while (ind < communities.length && notFound) {
//          val comm = communities(ind)
//          if (comm.contains(i) || comm.contains(j)) {
//            index = ind
//            notFound = false
//          }
//        }
        for (ind <- communities.indices) {
          val comm = communities(ind)
//          println(comm)
          if (comm.contains(i) || comm.contains(j)) {
//            println("changed")
            index = ind
//
          }
        }
        communities.remove(index)
        val community1 = dfs(i, scala.collection.mutable.Set[Int]())
        val community2 = dfs(j, scala.collection.mutable.Set[Int]())
        communities += (community1, community2)
        var newModu = 0.0
        for (comm <- communities) {
          newModu += calcModularity(comm)
        }
        modularity = newModu
        println(modularity)
        if (maxModu < modularity) {
          maxModu = modularity
          maxComm = communities.toList
        }
      }

    }
    println(maxModu)
    println(maxComm.length)
//    println(maxComm)
    val finalComm = scala.collection.mutable.ListBuffer.empty[List[Int]]
    for (comm <- maxComm) {
      finalComm += comm.toList.sorted
    }
    def sortByFirst(a: List[Int], b: List[Int]) : Boolean = {
      a.head < b.head
    }

    val finals = finalComm.sortWith(sortByFirst)

    val file = new File("Xiaoyu_Zheng_Community.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val it = finals.iterator
    while (it.hasNext) {
      var community = it.next()
      //      var size = point.length
      var i = 0
//      val comm = community.toList.sorted
      bw.write("[")
      for (node <- community) {
        if (i != 0) {
          bw.write(", ")
        }
        bw.write(node.toString)
        i += 1
      }
      bw.write("]")
      bw.write("\n")
    }
    bw.close()

    val end = System.nanoTime()
    println("Time: "+((end-start)/1e9d).toString+" sec")
  }

}
