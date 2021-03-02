import java.time.LocalDateTime
import java.util.Base64

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

object Post {
  def fromRow(row: Array[String]): Post = {
    Post(
      row(0).toInt,
      row(1).toInt,
      if (row(2) == "NULL") None else Some(LocalDateTime.parse(row(2), MyUtils.dateFormatter)),
      row(3).toInt,
      row(4).toInt,
      new String(Base64.getDecoder.decode(row(5))),
      if (row(6) == "NULL") None else Some(row(6).toInt),
      LocalDateTime.parse(row(7), MyUtils.dateFormatter),
      row(8),
      row(9).split("><").map(x => x.replace("<", "").replace(">", "")),
      row(10).toInt,
      row(11).toInt,
      row(12).toInt,
      if (row(13) == "NULL") None else Some(LocalDateTime.parse(row(13), MyUtils.dateFormatter))
    )
  }
}