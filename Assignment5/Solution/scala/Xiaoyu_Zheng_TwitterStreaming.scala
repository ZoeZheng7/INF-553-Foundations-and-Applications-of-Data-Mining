import twitter4j._
import twitter4j.conf.Configuration

import scala.collection.mutable.ListBuffer
import scala.util.Random


object TwitterStreaming {

  def main(args: Array[String]): Unit = {

    if (args.length != 0) {
      println(
        """
          |Usage:
          |cd $SPARK_HOME
          |bin/spark-submit --class TwitterStreaming Xiaoyu_Zheng_hw5.jar
        """.stripMargin)
      System.exit(-1)
    }

    val stream = new TwitterStreamFactory(Util.config).getInstance()
    stream.addListener(Util.myListener)
    stream.filter("#")
  }

}

object Util {

  var customerToken = "dF4iDYluulhXHEEDwhYDZcvlU"
  var customerSecret = "EAnV7RzKb32Z7FukgyzoWbi2gX6XKX9vpbhYU6cEZ0LBvi8ylO"
  var accessToken = "3076014876-N3WBMAgM4UE5JjoKcfYoB2dsi5qrMqWptlcgOho"
  var accessSecret = "0WgCe4jRdobSFFeaHAjinVZzK3Um2XOwtDBOiQ6eKCymh"

  val config: Configuration = new twitter4j.conf.ConfigurationBuilder()
    .setOAuthConsumerKey(customerToken)
    .setOAuthConsumerSecret(customerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(accessSecret)
    .build

  def myListener = new StatusListener {

    var count = 0
    var list: ListBuffer[String] = scala.collection.mutable.ListBuffer.empty[String]
    var sumi = 0
    var tags = scala.collection.mutable.HashMap.empty[String, Int]
    val random: Random.type = scala.util.Random

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

    override def onScrubGeo(l: Long, l1: Long) {}

    override def onStatus(status: Status): Unit = {
//      println(status.getText)
//      println()
      count += 1
      if (count > 100) {
        val rmd = random.nextInt(count)+1
        if (rmd < 100) {
          val statusOut = list.remove(rmd)
          sumi += status.getText.length - statusOut.length
          list.append(status.getText)
          val words = status.getText.split("\\s")
          var tagIn = scala.collection.mutable.ListBuffer.empty[String]
          for (word <- words) {
            if (word.length > 1 && word(0) == '#') {
              tagIn.append(word.substring(1))
            }
          }
          for (tag <- tagIn) {
            if (tags.contains(tag)) {
              tags(tag) += 1
            }
            else {
              tags(tag) = 1
            }
          }
          val wordsOut = statusOut.split("\\s")
          var tagOut = scala.collection.mutable.ListBuffer.empty[String]
          for (word <- wordsOut) {
            if (word.length > 1 && word(0) == '#') {
              tagOut.append(word.substring(1))
            }
          }
          for (tag <- tagOut) {
            tags(tag) -= 1
          }
          println("The number of the twitter from beginning: "+ count)
          println("Top 5 hot hashtags:")
          val order = tags.toSeq.sortBy(-_._2)
          val length = Math.min(5, order.length)
          for (i <- 0 until length) {
            val tmp = order(i)._1
            print(tmp)
            print(": ")
            println(tags(tmp))
          }
          println("The average length of the twitter is: "+1.0*sumi/100)
          println()
        }
      }
      else {
        list.append(status.getText)
        sumi += status.getText.length
        val words = status.getText.split("\\s")
        var tagIn = scala.collection.mutable.ListBuffer.empty[String]
        for (word <- words) {
          if (word.length > 1 && word(0) == '#') {
            tagIn.append(word.substring(1))
          }
        }
        for (tag <- tagIn) {
          if (tags.contains(tag)) {
            tags(tag) += 1
          }
          else {
            tags(tag) = 1
          }
        }
      }
    }

    override def onTrackLimitationNotice(i: Int) {}

    override def onStallWarning(stallWarning: StallWarning) {}

    override def onException(e: Exception): Unit = {
      e.printStackTrace()
    }
  }
}
