import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object DGIMAlgorithm {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val host = "localhost"
    val port = 9999
    val conf = new SparkConf().setMaster("local[4]").setAppName("DGIM")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream(host, port)

//    println("connected to the port")

    val words = lines.flatMap(_.split("\\s"))

    var buckets = scala.collection.mutable.ListBuffer.empty[(Int, Int)]
    var timestamp = 0
    var bits = ""
    var ones = 0

    def dgim(lis: RDD[String]): Unit = {
      val list = lis.collect()
      for (e <- list) {
        if (bits.length >= 1000) {
          val tmpBit = bits(0)
          bits = bits.substring(1)
          if (tmpBit.toString == "1") {
            ones -= 1
          }
        }
        timestamp += 1
        bits = bits + e
        if (buckets.length > 1) {
          if (buckets.last._2 < timestamp - 1000) {
            buckets.remove(buckets.length-1)
          }
        }
        if (e == "1") {
          ones += 1
          buckets.insert(0, (0, timestamp))
          var ind = 0
          while (ind+2 < buckets.length) {
            if ((buckets(ind)._1 == buckets(ind+1)._1) && (buckets(ind+2)._1 == buckets(ind+1)._1)) {
              val size = buckets(ind)._1+1
              val minTimeStamp = buckets(ind+1)._2
              buckets.remove(ind+1)
              buckets.remove(ind+1)
              buckets.insert(ind+1, (size, minTimeStamp))
            }
            ind += 1
          }
        }
//        println(buckets)
      }
      if (timestamp >= 1000) {
        var ind = 0
        var sum = 0.0
        for (bucket <- buckets) {
          if (ind != buckets.length-1) {
            sum += Math.pow(2.toDouble, bucket._1.toDouble)
          }
          else {
            sum += Math.pow(2.toDouble, bucket._1.toDouble)/2
//            sum += bucket._1/2
          }
          ind += 1
        }
        println("Estimated number of ones in the last 1000 bits: " + sum.toInt)
        println("Actual number of ones in the last 1000 bits: " + ones)
        println()
      }

    }

    words.foreachRDD(rdd => dgim(rdd))

    ssc.start()
    ssc.awaitTermination()

  }

}
