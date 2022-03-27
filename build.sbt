ThisBuild / organization := "it.yasp"
ThisBuild / version      := "0.0.1"
ThisBuild / scalaVersion := "2.11.12"

lazy val dependencies = new {
  val scoptV      = "4.0.1"
  val apacheTextV = "1.9"
  val circeV      = "0.10.1"
  val sparkV      = "2.4.7"
  val sparkXmlV   = "0.8.0"
  val scalaTestV  = "3.2.10"
  val scalaMockV  = "5.1.0"
  val h2dbV       = "1.4.200"

  val apacheText     = "org.apache.commons" % "commons-text"  % apacheTextV
  val scopt          = "com.github.scopt"  %% "scopt"         % scoptV
  val circeCore      = "io.circe"          %% "circe-core"    % circeV
  val circeGeneric   = "io.circe"          %% "circe-generic" % circeV
  val circeParser    = "io.circe"          %% "circe-parser"  % circeV
  val circeParserYml = "io.circe"          %% "circe-yaml"    % circeV
  val sparkSql       = "org.apache.spark"  %% "spark-sql"     % sparkV
  val sparkAvro      = "org.apache.spark"  %% "spark-avro"    % sparkV
  val sparkXml       = "com.databricks"    %% "spark-xml"     % sparkXmlV
  val scalactic      = "org.scalactic"     %% "scalactic"     % scalaTestV
  val scalaTest      = "org.scalatest"     %% "scalatest"     % scalaTestV
  val scalaMock      = "org.scalamock"     %% "scalamock"     % scalaMockV
  val h2db           = "com.h2database"     % "h2"            % h2dbV

}

lazy val root = (project in file("."))
  .aggregate(testKit, core, service, app)

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
      dependencies.scopt,
      dependencies.apacheText,
      dependencies.circeCore,
      dependencies.circeGeneric,
      dependencies.circeParser,
      dependencies.circeParserYml,
      dependencies.scalactic,
      dependencies.scalaTest % Test,
      dependencies.scalaMock % Test,
      dependencies.h2db      % Test
    )
  )
  .dependsOn(service, testKit % Test)
