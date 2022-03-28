package it.yasp.core.spark.model

/** Source Sumtype
  */
sealed trait Source extends Product with Serializable

object Source {

  /** A Csv Source Model
    * @param path:
    *   path of the Csv files
    * @param header:
    *   option that enable the file header on read
    * @param separator:
    *   option that enable different kind of separator on read
    */
  case class Csv(
      path: String,
      header: Boolean,
      separator: String
  ) extends Source

  /** A Parquet Source Model
    * @param path:
    *   path of the Parquet files
    * @param mergeSchema:
    *   option that enable the merge schema operation on read
    */
  case class Parquet(
      path: String,
      mergeSchema: Boolean
  ) extends Source

  /** A Json Source Model
    * @param path:
    *   path of the json files
    */
  case class Json(
      path: String
  ) extends Source

  /** An Avro Source Model
    * @param path:
    *   path of the avro files
    */
  case class Avro(
      path: String
  ) extends Source

  /** An Xml Source Model
    * @param path:
    *   path of the xml files
    * @param rowTag:
    *   the xml tag that should be consider as a row on read
    */
  case class Xml(
      path: String,
      rowTag: String
  ) extends Source

  /** A Jdbc Source Model
    * @param url:
    *   the url of the database
    * @param table:
    *   the table to read
    * @param credentials:
    *   an Optional BasicCredential configuration
    */
  case class Jdbc(
      url: String,
      table: String,
      credentials: Option[BasicCredentials]
  ) extends Source
}
