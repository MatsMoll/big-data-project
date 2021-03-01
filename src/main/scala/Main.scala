import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
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

object MyUtils {
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
}

object SimpleApp {
  def main(args: Array[String]) {
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
    // val badgesRdd = spark.read.options(Map("delimiter"->"\t", "header"->"true")).csv(config.badgesUri).rdd

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
    
    // println(postsRdd.count())
    // println(commentsRdd.count())
    // println(useresRdd.count())
    // println(badgesRdd.count())
    spark.stop()
  }
}