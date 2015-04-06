import sbt._
import sbt.Keys._

object Common {
  val commonSettings = Seq(
    organization := "com.github.mogproject",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.11.6",
    scalacOptions ++= Seq(
      "-encoding", "utf-8",
      "-target:jvm-1.7",
      "-deprecation",
      "-unchecked",
      "-Xlint",
      "-feature"
    ),
    javacOptions ++= Seq(
      "-encoding", "utf-8",
      "-source", "1.7",
      "-target", "1.7"
    ),
    resolvers ++= Dependencies.resolutionRepos
  )

  val testSettings = Seq(
    fork in Test := true,
    javaOptions in Test ++= sys.process.javaVmArguments.filter(a => Seq("-Xmx", "-Xms", "-XX").exists(a.startsWith))
  )

  val promptSettings = Seq(
    shellPrompt := { s => s"${Project.extract(s).currentProject.id}> "}
  )

  def createProject(name: String, path: Option[String] = None) = Project(name, file(path.getOrElse(name)))
    .settings(commonSettings ++ testSettings ++ promptSettings: _*)
}
