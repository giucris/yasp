ThisBuild / organization := "it.yasp"
ThisBuild / version := "0.0.1"
ThisBuild / scalaVersion := "2.12.10"

lazy val dependencies = new {
  val sparkSqlV  = "2.4.4"
  val scalaTestV = "3.2.10"

  val sparkSql  = "org.apache.spark" %% "spark-sql" % sparkSqlV
  val scalactic = "org.scalactic"    %% "scalactic" % scalaTestV
  val scalatest = "org.scalatest"    %% "scalatest" % scalaTestV
}

lazy val root = (project in file("."))

lazy val core = (project in file("yasp-core"))
  .settings(
    name := "yasp-core",
    libraryDependencies ++= Seq(
      dependencies.sparkSql,
      dependencies.scalactic,
      dependencies.scalatest % "test"
    )
  )
