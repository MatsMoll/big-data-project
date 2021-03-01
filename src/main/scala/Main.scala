import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Base64

case class ProjectFileConfig(postsUri: String, commentsUri: String, usersUri: String, badgesUri: String)
object Computations {
  def averageCharLengthBase64(base64rdd: RDD[String]): Double = {
    val strings = base64rdd.map(baseString => { 
      val data = Base64.getDecoder().decode(baseString)
      new String(data)
    })
    val charLength = strings.map(string => { 
      (string.toCharArray().length, 1)
    })
    .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    
    charLength._1.doubleValue() / charLength._2.doubleValue()
  }

  def averageCharLengthString(strings: RDD[String]): Double = {
    val charLength = strings.map(string => { 
      (string.toCharArray().length, 1)
    })
    .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    
    charLength._1.doubleValue() / charLength._2.doubleValue()
  }


}

object SimpleApp {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("Big Data Project")
      .master("local[1]")
      .getOrCreate()

    val config = ProjectFileConfig(
      "data/posts.csv",
      "data/comments.csv",
      "data/users.csv",
      "data/badges.csv"
    )
    val postsRdd = spark.sparkContext.textFile(config.postsUri).filter(x => !x.startsWith("\"Id\"")).map(x => x.split("\t"))
    val posts = postsRdd.map(row => Post.fromRow(row))

    val commentsRdd = spark.sparkContext.textFile(config.commentsUri).filter(x => !x.startsWith("\"PostId\"")).map(x => x.split("\t"))
    val useresRdd = spark.sparkContext.textFile(config.usersUri).filter(x => !x.startsWith("\"Id\"")).map(x => x.split("\t"))

    val users = useresRdd.map(row => User.fromRow(row))
    val usersMap = users.map(user => (user.id, user)).collect().toMap

    val averagePostCharLength = Computations.averageCharLengthBase64(postsRdd.map(x => x(5)))
    val averageCommentCharLength = Computations.averageCharLengthBase64(commentsRdd.map(x => x(2)))
    val averageQuestionCharLength = Computations.averageCharLengthString(postsRdd.map(x => x(8)))

    val oldestPost = posts.reduce((oldestPost, post) => {
      (post.creationDate, oldestPost.creationDate) match {
        case (Some(potenital), Some(oldestDate)) => 
          if (potenital.isBefore(oldestDate)) post else oldestPost
        case (Some(_), None) => post
        case _ => oldestPost
      }
    })
    val newestPost = posts.reduce((newestPost, post) => {
      (post.creationDate, newestPost.creationDate) match {
        case (Some(potenital), Some(newestDate)) => 
          if (newestDate.isBefore(potenital)) newestPost else post
        case (Some(_), None) => post
        case _ => newestPost
      }
    })
    println(newestPost)
    println(oldestPost)
    println(newestPost.ownerUserId.map(userID => usersMap(userID)))
    println(oldestPost.ownerUserId.map(userID => usersMap(userID)))

    println("Post", averagePostCharLength)
    println("Question", averageQuestionCharLength)
    println("Comment", averageCommentCharLength)

    // 2.3
    println("=== TASK 2.3 ===")
    val postss = posts
      .filter(p => p.postTypeId == 1 || p.postTypeId == 2)
      .filter(p => p.ownerUserId.isDefined)
      .filter(p => p.ownerUserId.get != -1)
      .map(p => (p.ownerUserId.get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)
      .foreach(println)
    println("=== USERS WITH MOST QUESTIONS AND ANSWERS COLLECTIVELY ===")

    val posta = posts
      .filter(p => p.postTypeId == 1)
      .filter(p => p.ownerUserId.isDefined)
      .filter(p => p.ownerUserId.get != -1)
      .map(p => (p.ownerUserId.get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)
      .foreach(println)
    println("===MOST QUESTION ONLY==")

    val postas = posts
      .filter(p => p.postTypeId == 2)
      .filter(p => p.ownerUserId.isDefined)
      .filter(p => p.ownerUserId.get != -1)
      .map(p => (p.ownerUserId.get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)
      .foreach(println)
    println("===MOST ANSWERS ONLY==")

    val badgesRdd = spark.sparkContext.textFile(config.badgesUri).filter(x => !x.startsWith("\"UserId\"")).map(x => x.split("\t"))
    val badges = badgesRdd.map(row => Badge.fromRow(row))

    // 2.4
    val badgesLessThan3 = badges.map(badge => (badge.userId, 1))
      .reduceByKey(_ + _)
      .filter{ case (_, badgeCount) => badgeCount < 3}
      .count();
    println("badges")
    println(badgesLessThan3)

    // 2.5
    val upvotesMean = users.map(u => u.upVotes).mean()
    val downvotesMean = users.map(u => u.downVotes).mean()

    println(s"upvotemean: $upvotesMean, downvotemean: $downvotesMean")

    val pearsonsTable = users.map(u => {
      val up = (u.upVotes - upvotesMean)
      val left = up * up
      val down = (u.downVotes - downvotesMean)
      val right = down * down
      val numerator = up * down

      (numerator, left, right)
    }).collect()

    pearsonsTable.take(20).foreach(println)

    val numeratorSum = pearsonsTable
      .map{case (numerator, _, _) => numerator}
      .sum
    println(s"numSum: ${numeratorSum.toString}")

    val left = pearsonsTable
      .map{case (_, left, _) => left}
      .sum
    println(s"left: ${left.toString}")

    val right = pearsonsTable
      .map{case (_, _, right) => right}
      .sum
    println(s"right: ${right.toString}")

    val res = (numeratorSum) / (math.sqrt(math.abs(left)) * (math.sqrt(math.abs(right))))
    println(s"correlation: ${res.toString}")
    // println(postsRdd.count())
    // println(commentsRdd.count())
    // println(useresRdd.count())
    // println(badgesRdd.count())
    spark.stop()
  }
}