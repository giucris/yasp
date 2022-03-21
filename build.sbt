ThisBuild / organization := "it.yasp"
ThisBuild / version      := "0.0.2"
ThisBuild / scalaVersion := "2.12.10"

lazy val dependencies = new {
  val sparkV     = "2.4.7"
  val sparkXmlV  = "0.8.0"
  val scalaTestV = "3.2.10"
  val scalaMockV = "5.1.0"
  val h2dbV      = "1.4.200"

  val sparkSql  = "org.apache.spark" %% "spark-sql"  % sparkV
  val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkV
  val sparkXml  = "com.databricks"   %% "spark-xml"  % sparkXmlV
  val scalactic = "org.scalactic"    %% "scalactic"  % scalaTestV
  val scalaTest = "org.scalatest"    %% "scalatest"  % scalaTestV
  val scalaMock = "org.scalamock"    %% "scalamock"  % scalaMockV
  val h2db      = "com.h2database"    % "h2"         % h2dbV

}

lazy val root = (project in file("."))
  .aggregate(testKit, core, service)

lazy val testKit = (project in file("yasp-testkit"))
  .settings(
    name := "yasp-testkit",
    libraryDependencies ++= Seq(
      dependencies.sparkSql,
      dependencies.scalactic,
      dependencies.scalaTest
    )
  )

lazy val core    = (project in file("yasp-core"))
  .settings(
    name := "yasp-core",
    libraryDependencies ++= Seq(
      dependencies.sparkSql,
      dependencies.sparkAvro,
      dependencies.sparkXml,
      dependencies.scalactic,
      dependencies.scalaTest % Test,
      dependencies.h2db      % Test
    )
  )
  .dependsOn(testKit % Test)

lazy val service = (project in file("yasp-service"))
  .settings(
    name := "yasp-service",
    libraryDependencies ++= Seq(
      dependencies.scalactic,
      dependencies.scalaTest % Test,
      dependencies.scalaMock % Test,
      dependencies.h2db      % Test
    )
  )
  .dependsOn(core, testKit % Test)

lazy val app     = (project in file("yasp-app"))
  .settings(
    name := "yasp-app",
    libraryDependencies ++= Seq(
      dependencies.scalactic,
      dependencies.scalaTest % Test,
      dependencies.scalaMock % Test,
      dependencies.h2db      % Test
    )
  )
  .dependsOn(service)
