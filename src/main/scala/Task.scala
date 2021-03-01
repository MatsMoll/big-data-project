import java.util.Base64

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
  def RDDRowCounts(postsRdd: RDD[Array[String]],
                   commentsRdd: RDD[Array[String]],
                   useresRdd: RDD[Array[String]],
                   badgesRdd: RDD[Array[String]]): Unit = {
    println(postsRdd.count())
    println(commentsRdd.count())
    println(useresRdd.count())
    println(badgesRdd.count())
  }

  // Task 2.1
  def AverageCharacterLengthInTexts(postsRdd: RDD[Array[String]],
                                    commentsRdd: RDD[Array[String]],
                                    useresRdd: RDD[Array[String]]): Unit = {
    val averagePostCharLength = Computations.averageCharLengthBase64(postsRdd.map(x => x(5)))
    val averageCommentCharLength = Computations.averageCharLengthBase64(commentsRdd.map(x => x(2)))
    val averageQuestionCharLength = Computations.averageCharLengthString(postsRdd.map(x => x(8)))


    println("Post", averagePostCharLength)
    println("Question", averageQuestionCharLength)
    println("Comment", averageCommentCharLength)
  }

  // Task 2.3
  def UserIdOfMostAnswers(posts: RDD[Post]): Unit = {
    println("=== TASK 2.3 ===")
    val postss = posts
      .filter(p => p.postTypeId == 1 || p.postTypeId == 2)
      .filter(p => p.ownerUserId.isDefined)
      .filter(p => p.ownerUserId.get != -1)
      .map(p => (p.ownerUserId.get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)
      .map{case (id, count) => println(s"(UserId:$id, count: $count)")}
    println("=== USERS WITH MOST QUESTIONS AND ANSWERS COLLECTIVELY ===\n")

    val posta = posts
      .filter(p => p.postTypeId == 1)
      .filter(p => p.ownerUserId.isDefined)
      .filter(p => p.ownerUserId.get != -1)
      .map(p => (p.ownerUserId.get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)
      .map{case (id, count) => println(s"(UserId:$id, count: $count)")}
    println("===MOST QUESTION ONLY==\n")

    val postas = posts
      .filter(p => p.postTypeId == 2)
      .filter(p => p.ownerUserId.isDefined)
      .filter(p => p.ownerUserId.get != -1)
      .map(p => (p.ownerUserId.get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)
      .map{case (id, count) => println(s"(UserId:$id, count: $count)")}
    println("===MOST ANSWERS ONLY==\n")
  }

  // Task 2.4
  def CountOfUsersWithLessThanThreeBadges(badges: RDD[Badge]): Long = {
    val badgesLessThan3 = badges.map(badge => (badge.userId, 1))
      .reduceByKey(_ + _)
      .filter{ case (_, badgeCount) => badgeCount < 3}
      .count()

    println(s"Task 2.4: Users with less than three badges: $badgesLessThan3\n")

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
      .map{case (numerator, _, _) => numerator}
      .sum
    //println(s"numSum: ${numeratorSum.toString}")

    val left = pearsonsTable
      .map{case (_, left, _) => left}
      .sum
    //println(s"left: ${left.toString}")

    val right = pearsonsTable
      .map{case (_, _, right) => right}
      .sum
    //println(s"right: ${right.toString}")

    val res = (numeratorSum) / (math.sqrt(math.abs(left)) * (math.sqrt(math.abs(right))))
    println(s"Task 2.5: r-coefficient: ${res.toString}\n")

    res
  }

}