val commonSettings = Seq(
  organization := "it.yasp",
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  version      := "0.0.1",
  scalaVersion := "2.12.15",
  scalacOptions ++= Settings.scalaCompilerOptions,
  developers   := List(
    Developer(
      "giucris",
      "Giuseppe Cristiano",
      "giucristiano89@gmail.com",
      url("https://github.com/giucris")
    )
  )
)

lazy val dependencies = new {
  val scoptV                = "4.0.1"
  val apacheTextV           = "1.9"
  val catsV                 = "2.0.0"
  val circeV                = "0.12.0"
  val sparkV                = "3.3.0"
  val sparkXmlV             = "0.14.0"
  val scalaTestV            = "3.2.10"
  val scalaMockV            = "5.1.0"
  val h2dbV                 = "1.4.200"
  val typeSafeScalaLoggingV = "3.9.5"
  val logbackV              = "1.3.1"

  val apacheText           = "org.apache.commons"          % "commons-text"    % apacheTextV
  val scopt                = "com.github.scopt"           %% "scopt"           % scoptV
  val catsCore             = "org.typelevel"              %% "cats-core"       % catsV
  val circeCore            = "io.circe"                   %% "circe-core"      % circeV
  val circeGeneric         = "io.circe"                   %% "circe-generic"   % circeV
  val circeParser          = "io.circe"                   %% "circe-parser"    % circeV
  val circeParserYml       = "io.circe"                   %% "circe-yaml"      % circeV
  val sparkSql             = "org.apache.spark"           %% "spark-sql"       % sparkV
  val sparkAvro            = "org.apache.spark"           %% "spark-avro"      % sparkV
  val sparkHive            = "org.apache.spark"           %% "spark-hive"      % sparkV
  val sparkXml             = "com.databricks"             %% "spark-xml"       % sparkXmlV
  val typeSafeScalaLogging = "com.typesafe.scala-logging" %% "scala-logging"   % typeSafeScalaLoggingV
  val logback              = "ch.qos.logback"              % "logback-classic" % logbackV
  val scalactic            = "org.scalactic"              %% "scalactic"       % scalaTestV
  val scalaTest            = "org.scalatest"              %% "scalatest"       % scalaTestV
  val scalaMock            = "org.scalamock"              %% "scalamock"       % scalaMockV
  val h2db                 = "com.h2database"              % "h2"              % h2dbV

}

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(Settings.wartRemover)
  .aggregate(testKit, core, service, app)

lazy val testKit = (project in file("yasp-testkit"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "yasp-testkit",
    Settings.wartRemover,
    libraryDependencies ++= Seq(
      dependencies.sparkSql,
      dependencies.scalactic,
      dependencies.scalaTest
    )
  )

lazy val core    = (project in file("yasp-core"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "yasp-core",
    Settings.wartRemover,
    libraryDependencies ++= Seq(
      dependencies.scalactic,
      dependencies.typeSafeScalaLogging,
      dependencies.logback,
      dependencies.sparkSql,
      dependencies.sparkAvro,
      dependencies.sparkXml,
      dependencies.sparkHive,
      dependencies.scalaTest % Test,
      dependencies.scalaMock % Test,
      dependencies.h2db      % Test
    )
  )
  .dependsOn(testKit % Test)

lazy val service = (project in file("yasp-service"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "yasp-service",
    Settings.wartRemover,
    libraryDependencies ++= Seq(
      dependencies.catsCore,
      dependencies.scalactic,
      dependencies.scalaTest % Test,
      dependencies.scalaMock % Test,
      dependencies.h2db      % Test
    )
  )
  .dependsOn(core, testKit % Test)

lazy val app     = (project in file("yasp-app"))
  .settings(commonSettings)
  .settings(
    name := "yasp-app",
    Settings.wartRemover,
    Settings.appAssembly,
    libraryDependencies ++= Seq(
      dependencies.scopt,
      dependencies.apacheText,
      dependencies.catsCore,
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
