package it.yasp.core.spark.model

sealed trait DataSource

object DataSource {

  case class Csv(
      paths: Seq[String],
      header: Boolean,
      separator: String
  ) extends DataSource

  case class Parquet(
      paths: Seq[String],
      mergeSchema: Boolean
  ) extends DataSource

  case class Json(
      paths: Seq[String]
  ) extends DataSource

  case class Avro(
      paths: Seq[String]
  ) extends DataSource

  case class Xml(
      paths: Seq[String],
      rowTag: String
  ) extends DataSource

  case class Jdbc(
      url: String,
      table: String,
      credentials: Option[BasicCredentials]
  ) extends DataSource
}
