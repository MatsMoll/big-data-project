import java.util.Base64

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import scala.math.log10
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.parquet.filter2.predicate.Operators.Column

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

object Task {

  // 1.1 - 1.4 in Setup.scala

  // Task 1.5
  def countRDDRows(postsRdd: RDD[Array[String]],
                   commentsRdd: RDD[Array[String]],
                   usersRdd: RDD[Array[String]],
                   badgesRdd: RDD[Array[String]]): Unit = {
    println("=== TASKS 1.1 through 1.4 START ===\n")
    println(s"Posts Rows: ${postsRdd.count()}")
    println(s"Comments Rows: ${commentsRdd.count()}")
    println(s"Users Rows: ${usersRdd.count()}")
    println(s"Badges Rows: ${badgesRdd.count()}")
    println("\n=== TASKS 1.1 through 1.4 END ===\n")
  }

  // Task 2.1
  def averageCharacterLengthInTexts(postsRdd: RDD[Array[String]],
                                    commentsRdd: RDD[Array[String]],
                                    useresRdd: RDD[Array[String]]): Unit = {
    val averagePostCharLength = Computations.averageCharLengthBase64(postsRdd.map(x => x(5)))
    val averageCommentCharLength = Computations.averageCharLengthBase64(commentsRdd.map(x => x(2)))
    val averageQuestionCharLength = Computations.averageCharLengthString(postsRdd.map(x => x(8)))


    println("Post", averagePostCharLength)
    println("Question", averageQuestionCharLength)
    println("Comment", averageCommentCharLength)
  }

  // Task 2.2
  def oldestAndNewestQuestions(posts: RDD[Post], users: RDD[User]): Unit = {
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
  }

  // Task 2.3
  def userIdOfMostQuestionsAndAnswersRespectively(posts: RDD[Post]): Unit = {
    println("=== TASK 2.3 ===")
    val mostQuestions = posts
      .filter(p => p.postTypeId == 1)
      .filter(p => p.ownerUserId.isDefined)
      .filter(p => p.ownerUserId.get != -1)
      .map(p => (p.ownerUserId.get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(1)
      .map { case (id, count) => println(s"(UserId:$id, count: $count)") }
    println("===MOST QUESTION ONLY==\n")

    val mostAnswers = posts
      .filter(p => p.postTypeId == 2)
      .filter(p => p.ownerUserId.isDefined)
      .filter(p => p.ownerUserId.get != -1)
      .map(p => (p.ownerUserId.get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(1)
      .map { case (id, count) => println(s"(UserId:$id, count: $count)") }
    println("===MOST ANSWERS ONLY==\n")
  }

  // Task 2.4
  def CountOfUsersWithLessThanThreeBadges(badges: RDD[Badge]): Long = {
    val badgesLessThan3 = badges.map(badge => (badge.userId, 1))
      .reduceByKey(_ + _)
      .filter { case (_, badgeCount) => badgeCount < 3 }
      .count()

    println(s"=== Task 2.4: Users with less than three " +
      s"badges: $badgesLessThan3\n ===")

    badgesLessThan3
  }

  // Task 2.5
  def UpvoteDownvotePearsonCorrelationCoefficient(users: RDD[User]): Double = {
    val upvotesMean = users.map(u => u.upVotes).mean()
    val downvotesMean = users.map(u => u.downVotes).mean()

    //println(s"upvotemean: $upvotesMean, downvotemean: $downvotesMean")

    val pearsonsTable = users.map(u => {
      val up = (u.upVotes - upvotesMean)
      val left = up * up
      val down = (u.downVotes - downvotesMean)
      val right = down * down
      val numerator = up * down

      (numerator, left, right)
    }).collect()

    val numeratorSum = pearsonsTable
      .map { case (numerator, _, _) => numerator }
      .sum
    //println(s"numSum: ${numeratorSum.toString}")

    val left = pearsonsTable
      .map { case (_, left, _) => left }
      .sum
    //println(s"left: ${left.toString}")

    val right = pearsonsTable
      .map { case (_, _, right) => right }
      .sum
    //println(s"right: ${right.toString}")
    val denominator = left * right;


    val res =
      (numeratorSum) /
        (math.sqrt(denominator))
    println(s"=== Task 2.5: r-coefficient: ${res.toString}\n ===")

    res
  }

  // Task 2.6
  def userEntropy(comments: RDD[Comment]): Double = {
    val numberOfComments = comments.count()
    val userComments = comments.groupBy(comment => comment.userId).cache()

    val usersEntropy = userComments
      .map({ case (_, postIDs: Iterable[Comment]) =>
        postIDs.count(_ => true).toDouble / numberOfComments.toDouble
      })
      .reduce({ case (sum, value) => sum - value * log10(value) / log10(2) })

    println(s"=== Task 2.6: users comment entropy: ${usersEntropy}\n ===")

    usersEntropy
  }


  // Task 3.1
  def userCommentGraph(comments: RDD[Comment], posts: RDD[Post], sparkContext: SparkContext): Graph[Int, Long] = {
    val postOwner = posts.flatMap(post => post.ownerUserId.map(ownerId => (post.id, ownerId))).collect().toMap

    // Filtering on userId > 1 as some posts has
    val validUserComments = comments.filter(comment => comment.userId >= 1)
    val userComments = validUserComments.groupBy(comment => comment.userId).cache()

    val rawEdges = validUserComments.flatMap(comment => postOwner.get(comment.postId)
      .map(ownerId => (comment.userId, ownerId)))
      .countByValue()

    val edges = sparkContext.parallelize(rawEdges.map({ case (userIDs, count) => Edge(userIDs._1, userIDs._2, count) }).toSeq)

    val nodes: RDD[(VertexId, Int)] = userComments.map(user => (user._1, user._1))
    Graph(nodes, edges, -1)
  }

  def dataframeFromGraph(graph: Graph[Int, Long], spark: SparkSession): DataFrame =
    spark.createDataFrame(graph.triplets.map(trip => (trip.srcId, trip.attr, trip.dstId)))
      .toDF(SQLStrings.commentUserID, SQLStrings.numberOfComments, SQLStrings.postUserID)

  // Task 3.2
  def userIDsWithMostComments(dataframe: DataFrame, amount: Int = 10): Dataset[Row] = {

    println("=== Task 3.2 ===")

    val mostComments = dataframe.groupBy(col(SQLStrings.commentUserID))
      .agg(sum(SQLStrings.numberOfComments).as(SQLStrings.numberOfCommentsSum))
      .sort(col(SQLStrings.numberOfCommentsSum).desc)
      .limit(10)

    mostComments.show()
    mostComments
  }

  def usersWithMostCommentsOnTheirPost(dataframe: DataFrame, users: RDD[User], spark: SparkSession, amount: Int = 10): Dataset[Row] = {

    val usersDataFrame = spark.createDataFrame(users).as(SQLStrings.usersTable)

    val mostComments = dataframe.groupBy(col(SQLStrings.postUserID))
      .agg(sum(SQLStrings.numberOfComments).as(SQLStrings.numberOfCommentsSum))
      .join(usersDataFrame, dataframe(SQLStrings.postUserID) === usersDataFrame(SQLStrings.userID))
      .sort(col(SQLStrings.numberOfCommentsSum).desc)
      .limit(amount)

    mostComments.show()
    mostComments
  }
}