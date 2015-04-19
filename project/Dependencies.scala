import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
  )

  def compile(deps: ModuleID*): Seq[ModuleID] = deps map (_ % "compile")

  def provided(deps: ModuleID*): Seq[ModuleID] = deps map (_ % "provided")

  def test(deps: ModuleID*): Seq[ModuleID] = deps map (_ % "test")

  def runtime(deps: ModuleID*): Seq[ModuleID] = deps map (_ % "runtime")

  def container(deps: ModuleID*): Seq[ModuleID] = deps map (_ % "container")


  // Versions
  val redisClientVersion = "2.15"

  // Libraries
  val redisClient = "net.debasishg" %% "redisclient" % redisClientVersion

  val logback = "ch.qos.logback" % "logback-classic" % "1.1.3"

  val scalatest = "org.scalatest" %% "scalatest" % "2.2.4"
  val scalacheck = "org.scalacheck" %% "scalacheck" % "1.12.2"
}
