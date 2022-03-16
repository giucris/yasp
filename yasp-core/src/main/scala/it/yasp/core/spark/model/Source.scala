package it.yasp.core.spark.model

sealed trait Source extends Product with Serializable

object Source {

  case class Csv(
      paths: Seq[String],
      header: Boolean,
      separator: String
  ) extends Source

  case class Parquet(
      paths: Seq[String],
      mergeSchema: Boolean
  ) extends Source

  case class Json(
      paths: Seq[String]
  ) extends Source

  case class Avro(
      paths: Seq[String]
  ) extends Source

  case class Xml(
      paths: Seq[String],
      rowTag: String
  ) extends Source

  case class Jdbc(
      url: String,
      table: String,
      credentials: Option[BasicCredentials]
  ) extends Source
}
