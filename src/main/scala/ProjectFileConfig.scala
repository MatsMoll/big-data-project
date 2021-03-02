case class ProjectFileConfig(postsUri: String,
                             commentsUri: String,
                             usersUri: String,
                             badgesUri: String, 
                             saveCommentPostUri: String)

import java.nio.file.{Paths, Files}

object ProjectFileConfig {
  def parseArgs(args: Array[String]): ProjectFileConfig = {
    if (args.length != 2) {
        throw new Exception("Missing input_path argument")
    }
    val inputPathIndex = args.indexWhere(arg => arg == "--input_path")
    if (inputPathIndex + 1 >= args.length) {
        throw new Exception("Missing argument after input_path")
    }
    val inputPath = args(inputPathIndex + 1)

    if (Files.exists(Paths.get(s"$inputPath/posts.csv"))) {
        ProjectFileConfig(
            s"$inputPath/posts.csv",
            s"$inputPath/comments.csv",
            s"$inputPath/users.csv",
            s"$inputPath/badges.csv",
            s"$inputPath/commentPosts.csv"
        )
    } else {
        ProjectFileConfig(
            s"$inputPath/posts.csv.gz",
            s"$inputPath/comments.csv.gz",
            s"$inputPath/users.csv.gz",
            s"$inputPath/badges.csv.gz",
            s"$inputPath/commentPosts.csv"
        )
    }
  }
}