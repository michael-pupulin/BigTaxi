ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "BigTaxi",
    libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
    libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" ,
      libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.8.4"
  )
