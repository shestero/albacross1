ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file(".")).settings(
    name := "albacross1"
  )

val sparkVersion = "3.3.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "mysql" % "mysql-connector-java" % "8.0.+", // "8.0.31"
  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.5.3" excludeAll ExclusionRule(organization = "org.apache.spark"),
  "org.typelevel" %% "cats-core" % "2.9.0" // used only to add two tuples
)

Compile / mainClass := Some("Main")

