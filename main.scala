import org.apache.spark.sql.SparkSession
import java.io.File
import java.util.Base64
import java.nio.charset.StandardCharsets
import org.apache.spark.rdd.RDD
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class ProjectFileConfig(postsUri: String, commentsUri: String, usersUri: String, badgesUri: String)
case class Post(
  id: Int, 
  postTypeId: Int, 
  creationDate: Option[LocalDateTime], 
  score: Int,
  viewCount: Int, 
  body: String, 
  ownerUserId: Option[Int], 
  lastActivityDate: LocalDateTime, 
  title: String, 
  tags: Array[String], 
  answerCount: Int, 
  commentCount: Int, 
  favoriteCount: Int, 
  closeDate: Option[LocalDateTime]
)

object Computations {
  def averageCharLengthBase64(base64rdd: RDD[String]): Double = {
    val strings = base64rdd.map(baseString => { 
      val data = Base64.getDecoder().decode(baseString.replace("\r\n", ""))
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
    val posts = postsRdd.map(row => {
      Post(
        row(0).toInt,
        row(1).toInt,
        if (row(2) == "NULL") None else Some(LocalDateTime.parse(row(2), MyUtils.dateFormatter)),
        row(3).toInt,
        row(4).toInt,
        new String(Base64.getDecoder().decode(row(5))),
        if (row(6) == "NULL") None else Some(row(6).toInt),
        LocalDateTime.parse(row(7), MyUtils.dateFormatter),
        row(8),
        row(9).split("><").map(x => x.replace("<", "").replace(">", "")),
        row(10).toInt,
        row(11).toInt,
        row(12).toInt,
        if (row(13) == "NULL") None else Some(LocalDateTime.parse(row(13), MyUtils.dateFormatter))
      )
    })

    val commentsRdd = spark.sparkContext.textFile(config.commentsUri).filter(x => !x.startsWith("\"PostId\"")).map(x => x.split("\t"))
    // val useresRdd = spark.sparkContext.textFile(config.commentsUri).filter(x => !x.startsWith("\"PostId\"")).map(x => x.split("\t"))
    // val badgesRdd = spark.read.options(Map("delimiter"->"\t", "header"->"true")).csv(config.badgesUri).rdd

    println(postsRdd.map(x => x(8)).first())
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
    print(oldestPost)

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