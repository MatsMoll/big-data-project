import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.util.Base64
import scala.math.log10
import org.apache.spark.graphx._

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

    val badgesRdd = spark.sparkContext.textFile(config.badgesUri).filter(x => !x.startsWith("\"UserId\"")).map(x => x.split("\t"))
    val badges = badgesRdd.map(row => Badge.fromRow(row))

    // 2.4
    val badgesLessThan3 = badges.map(badge => (badge.userId, 1))
      .reduceByKey(_ + _)
      .filter{ case (_, badgeCount) => badgeCount < 3}
      .count();
    println("badges")
    println(badgesLessThan3)


    // 2.6
    val comments = commentsRdd.map(row => Comment.fromRow(row))
    val numberOfComments = comments.count()
    val userComments = comments.groupBy(comment => comment.userId)

    val usersEntropy = userComments
      .map({case (_, postIDs: Iterable[Comment]) =>
        postIDs.count(_ => true).toDouble / numberOfComments.toDouble
      })
      .reduce({case (sum, value) => sum - value * log10(value) / log10(2)})

    println(usersEntropy)


    // 3.1

    val postOwner = posts.flatMap(post => post.ownerUserId.map(ownerId => (post.id, ownerId))).collect().toMap
    
    val rawEdges = comments.flatMap(comment => postOwner.get(comment.postId)
        .map(ownerId =>  (comment.userId, ownerId) ) )
        .countByValue()

    val edges = spark.sparkContext.parallelize(rawEdges.map({case (userIDs, count) => Edge(userIDs._1, userIDs._2, count)}).toSeq)

    val nodes: RDD[(VertexId, Int)] = userComments.map(user => (user._1, user._1))
    val postGraph = Graph(nodes, edges)

    println(postGraph)
    
    // println(postsRdd.count())
    // println(commentsRdd.count())
    // println(useresRdd.count())
    // println(badgesRdd.count())
    spark.stop()
  }
}
