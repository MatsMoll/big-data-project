import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Base64
                             
object SimpleApp {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Big Data Project")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("OFF")

    val config = ProjectFileConfig.parseArgs(args)
    
    // TASK [1.1, 1.2, 1.3, 1.4]
    val badgesRdd = Setup.LoadBadgesRDD(spark, config)
    val postsRdd = Setup.LoadPostsRDD(spark, config)
    val commentsRdd = Setup.LoadCommentsRDD(spark, config)
    val useresRdd = Setup.LoadUseresRDD(spark, config)

    //
    val badges = badgesRdd.map(row => Badge.fromRow(row))
    val posts = postsRdd.map(row => Post.fromRow(row))
    val users = useresRdd.map(row => User.fromRow(row))
    val comments = commentsRdd.map(row => Comment.fromRow(row))

    // 2.1
    Task.countRDDRows(postsRdd, commentsRdd, useresRdd, badgesRdd)

    // 2.2
    Task.oldestAndNewestQuestions(posts, users)

    // 2.3
    Task.userIdOfMostQuestionsAndAnswersRespectively(posts)
    // 2.4
    Task.CountOfUsersWithLessThanThreeBadges(badges)
    // 2.5
    Task.UpvoteDownvotePearsonCorrelationCoefficient(users)

    // 2.6
    Task.userEntropy(comments)

    // 3.1
    val graph = Task.userCommentGraph(comments, posts, sc)

    // 3.2
    val dataframe = Task.dataframeFromGraph(graph, spark)

    // 3.3
    Task.userIDsWithMostComments(dataframe)
    
    // 3.4
    Task.usersWithMostCommentsOnTheirPost(dataframe, users, spark)
    
    // 3.5
    dataframe.write.csv("data/comments-posts.csv")

    spark.stop()
  }
}
