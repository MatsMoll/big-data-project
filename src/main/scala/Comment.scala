import java.time.LocalDateTime
import java.util.Base64

final case class Comment(
    postId: Int, 
    score: Int, 
    text: String, 
    creationDate: LocalDateTime, 
    userId: Int
)

object Comment {
    def fromRow(row: Array[String]): Comment = {
        Comment(
            row(0).toInt,
            row(1).toInt,
            new String(Base64.getDecoder().decode(row(2))),
            LocalDateTime.parse(row(3), MyUtils.dateFormatter),
            row(4).toInt
        )
    }
}