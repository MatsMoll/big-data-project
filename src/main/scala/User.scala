import java.time.LocalDateTime

case class User(
                 id: Int,
                 reprutation: String,
                 createdAt: Option[LocalDateTime],
                 displayName: String,
                 lastAccessDate: Option[LocalDateTime],
                 aboutMe: Option[String],
                 views: Option[Int],
                 upVotes: Int,
                 downVotes: Int
               )

object User {
  def fromRow(row: Array[String]): User = {
    User(
      row(0).toInt,
      row(1),
      if (row(2) == "NULL") None else Some(LocalDateTime.parse(row(2), MyUtils.dateFormatter)),
      row(3),
      if (row(4) == "\"") None else Some(LocalDateTime.parse(row(4), MyUtils.dateFormatter)),
      if (row(5) == "NULL") None else Some(row(5)),
      if (row(6) == "NULL") None else Some(row(6).toInt),
      row(7).toInt,
      row(8).toInt
    )
  }
}

