ThisBuild / organization := "it.yasp"
ThisBuild / version      := "0.0.1"
ThisBuild / scalaVersion := "2.12.10"

lazy val dependencies = new {
  val sparkSqlV  = "2.4.7"
  val scalaTestV = "3.2.10"
  val derbyV     = "10.14.2.0"

  val sparkSql  = "org.apache.spark" %% "spark-sql" % sparkSqlV
  val scalactic = "org.scalactic"    %% "scalactic" % scalaTestV
  val scalatest = "org.scalatest"    %% "scalatest" % scalaTestV
  val derby     = "org.apache.derby"  % "derby"     % derbyV
}

lazy val root = (project in file("."))
  .aggregate(testKit, core)

lazy val testKit = (project in file("yasp-testkit"))
  .settings(
    name := "yasp-testkit",
    libraryDependencies ++= Seq(
      dependencies.sparkSql,
      dependencies.scalactic,
      dependencies.scalatest
    )
  )

lazy val core = (project in file("yasp-core"))
  .settings(
    name := "yasp-core",
    libraryDependencies ++= Seq(
      dependencies.sparkSql,
      dependencies.scalactic,
      dependencies.scalatest % Test,
      dependencies.derby     % Test
    )
  )
  .dependsOn(testKit % "test")
