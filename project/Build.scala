import sbt._
import sbt.Keys._

object Build extends Build {

  import Dependencies._

  // Projects
  lazy val root = Common.createProject("scala-redis-mock", Some("."))
    .settings(
      libraryDependencies ++=
        compile(redisClient) ++
          test(scalatest, scalacheck)
    )
    .settings(
      initialCommands in console :=
        """
        import com.github.mogproject.redismock._
        val r = new MockRedisClient("localhost", 6379)
        """
    )
}
