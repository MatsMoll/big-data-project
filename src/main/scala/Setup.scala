import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Setup {
  // Task 1.1
  def LoadBadgesRDD(spark: SparkSession, config: ProjectFileConfig): RDD[Array[String]] = {
    spark.sparkContext
      .textFile(config.badgesUri)
      .filter(x => !x.startsWith("\"UserId\"")).map(x => x.split("\t"))
  }

  def LoadPostsRDD(spark: SparkSession, config: ProjectFileConfig)
  : RDD[Array[String]] = {
    spark.sparkContext
      .textFile(config.postsUri)
      .filter(x => !x.startsWith("\"Id\"")).map(x => x.split("\t"))
  }

  def LoadCommentsRDD(spark: SparkSession, config: ProjectFileConfig): RDD[Array[String]] = {
    spark.sparkContext
      .textFile(config.commentsUri)
      .filter(x => !x.startsWith("\"PostId\"")).map(x => x.split("\t"))
  }

  def LoadUseresRDD(spark: SparkSession, config: ProjectFileConfig)
  : RDD[Array[String]] = {
    spark.sparkContext
      .textFile(config.usersUri)
      .filter(x => !x.startsWith("\"Id\"")).map(x => x.split("\t"))
  }
}
