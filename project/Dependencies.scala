import sbt._

object Dependencies {

  val scalaTestV: String            = "3.2.10"
  val scalaMockV: String            = "5.1.0"
  val h2dbV: String                 = "1.4.200"
  val typeSafeScalaLoggingV: String = "3.9.5"
  val logbackV: String              = "1.3.1"
  val scoptV: String                = "4.0.1"
  val apacheTextV: String           = "1.9"
  val catsV: String                 = "2.0.0"
  val circeV: String                = "0.12.0"
  val sparkXmlV: String             = "0.14.0"
  val sparkIcebergV: String         = "0.14.1"

  // Cross Build based on SparkVersion
  val sparkDeltaV: String = Settings.yaspSparkVersion match {
    case v if v.startsWith("3.2") => "2.0.0"
    case v if v.startsWith("3.1") => "1.0.1"
    case v if v.startsWith("3.0") => "0.8.0"
    case _                        => "2.1.0" // Default Spark 3.3
  }

  // Cross Build based on SparkVersion
  val sparkIcebergPackage: String = Settings.yaspSparkVersion match {
    case v if v.startsWith("3.2") => "iceberg-spark-runtime-3.2_2.12"
    case v if v.startsWith("3.1") => "iceberg-spark-runtime-3.1_2.12"
    case v if v.startsWith("3.0") => "iceberg-spark3-runtime"
    case _                        => "2.1.0" // Default Spark 3.3
  }

  val apacheText           = "org.apache.commons"          % "commons-text"      % apacheTextV
  val scopt                = "com.github.scopt"           %% "scopt"             % scoptV
  val catsCore             = "org.typelevel"              %% "cats-core"         % catsV
  val circeCore            = "io.circe"                   %% "circe-core"        % circeV
  val circeGeneric         = "io.circe"                   %% "circe-generic"     % circeV
  val circeParser          = "io.circe"                   %% "circe-parser"      % circeV
  val circeParserYml       = "io.circe"                   %% "circe-yaml"        % circeV
  val scalactic            = "org.scalactic"              %% "scalactic"         % scalaTestV
  val scalaTest            = "org.scalatest"              %% "scalatest"         % scalaTestV
  val scalaMock            = "org.scalamock"              %% "scalamock"         % scalaMockV
  val typeSafeScalaLogging = "com.typesafe.scala-logging" %% "scala-logging"     % typeSafeScalaLoggingV
  val logback              = "ch.qos.logback"              % "logback-classic"   % logbackV
  val h2db                 = "com.h2database"              % "h2"                % h2dbV
  val sparkSql             = "org.apache.spark"           %% "spark-sql"         % Settings.yaspSparkVersion
  val sparkAvro            = "org.apache.spark"           %% "spark-avro"        % Settings.yaspSparkVersion
  val sparkHive            = "org.apache.spark"           %% "spark-hive"        % Settings.yaspSparkVersion
  val sparkXml             = "com.databricks"             %% "spark-xml"         % sparkXmlV
  val sparkDelta           = "io.delta"                   %% "delta-core"        % sparkDeltaV
  val sparkIceberg         = "org.apache.iceberg"          % sparkIcebergPackage % sparkIcebergV
}
