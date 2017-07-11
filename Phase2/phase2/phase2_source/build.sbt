import sbt.Keys._
import sun.security.tools.PathList
lazy val root = (project in file(".")).
  settings(
    name := "scala_code",
    version := "0.1",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("io.purush.spark.giscup.script.CellScript")
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1" % "provided"

resolvers += Resolver.mavenLocal

//// META-INF discarding
//assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//{
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}
//}