import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Base64

case class ProjectFileConfig(postsUri: String, commentsUri: String, usersUri: String, badgesUri: String)


object SimpleApp {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.FATAL)

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

    // TASK [1.1, 1.2, 1.3, 1.4]
    val badgesRdd = Setup.LoadBadgesRDD(spark, config)
    val postsRdd = Setup.LoadPostsRDD(spark, config)
    val commentsRdd = Setup.LoadCommentsRDD(spark, config)
    val useresRdd = Setup.LoadUseresRDD(spark, config)

    //
    val badges = badgesRdd.map(row => Badge.fromRow(row))
    val posts = postsRdd.map(row => Post.fromRow(row))
    val users = useresRdd.map(row => User.fromRow(row))

    val usersMap = users.map(user => (user.id, user)).collect().toMap

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


    // 2.1
    Task.RDDRowCounts(postsRdd, commentsRdd, useresRdd, badgesRdd)

    // 2.2


    // 2.3
    Task.UserIdOfMostAnswers(posts)
    // 2.4
    Task.CountOfUsersWithLessThanThreeBadges(badges)
    // 2.5
    Task.UpvoteDownvotePearsonCorrelationCoefficient(users)

    spark.stop()
  }
}