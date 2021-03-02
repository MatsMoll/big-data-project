import java.time.LocalDateTime
import java.sql.Date

// Need to use Date in order to make it possible to convert to a Dataframe
case class User(
                 id: Int,
                 reputation: Int,
                 createdAt: Option[Date],
                 displayName: String,
                 lastAccessDate: Option[Date],
                 aboutMe: Option[String],
                 views: Option[Int],
                 upVotes: Int,
                 downVotes: Int
               )

object User {
  def fromRow(row: Array[String]): User = {
    User(
      row(0).toInt,
      row(1).toInt,
      if (row(2) == "NULL") None else Some(Date.valueOf(LocalDateTime.parse(row(2), MyUtils.dateFormatter).toLocalDate())),
      row(3),
      if (row(4) == "\"") None else Some(Date.valueOf(LocalDateTime.parse(row(4), MyUtils.dateFormatter).toLocalDate())),
      if (row(5) == "NULL") None else Some(row(5)),
      if (row(6) == "NULL") None else Some(row(6).toInt),
      row(7).toInt,
      row(8).toInt
    )
  }
}

