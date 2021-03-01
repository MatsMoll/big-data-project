import java.time.LocalDateTime

case class Badge(
                 userId: Int,
                 name: String,
                 date: Option[LocalDateTime],
                 classNumber: Int
               )

object Badge {
  def fromRow(row: Array[String]): Badge = {
    Badge(
      row(0).toInt,
      row(1).toString,
      if (row(2) == "NULL") None else Some(LocalDateTime.parse(row(2), MyUtils.dateFormatter)),
      row(3).toInt
    )
  }
}