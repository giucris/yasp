package it.yasp.core.spark.model

/** Source Sumtype
  */
sealed trait Source extends Product with Serializable

object Source {

  /** A Csv Source Model
    * @param path:
    *   path of the Csv files
    * @param options:
    *   An optional map of Csv read configuration. For a complete list of valid configuration you
    *   can check at the official spark documentation
    *   (https://spark.apache.org/docs/2.4.7/api/java/org/apache/spark/sql/DataFrameReader.html#csv-java.lang.String...-).
    */
  case class Csv(
      path: String,
      options: Option[Map[String, String]]
  ) extends Source

  /** A Json Source Model
    * @param path:
    *   path of the json files
    * @param options:
    *   An optional map of Json read configuration. For a complete list of valid configuration you
    *   can check at the official spark documentation
    *   (https://spark.apache.org/docs/2.4.7/api/java/org/apache/spark/sql/DataFrameReader.html#json-java.lang.String...-).
    */
  case class Json(
      path: String,
      options: Option[Map[String, String]]
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

  /** An Orc Source Model
    * @param path:
    *   path of the Orc files
    */
  case class Orc(
      path: String
  ) extends Source

  /** An Avro Source Model
    * @param path:
    *   path of the avro files
    * @param options:
    *   An optional map of Avro read configuration. For a complete list of valid configuration you
    *   can check at the official spark documentation
    *   (https://spark.apache.org/docs/2.4.7/sql-data-sources-avro.html)
    */
  case class Avro(
      path: String,
      options: Option[Map[String, String]]
  ) extends Source

  /** An Xml Source Model
    * @param path:
    *   path of the xml files
    * @param options:
    *   An optional map of Xml read configuration. For a complete list of valid configuration you
    *   can check at the official databricks spark-xml github repository.
    *   (https://github.com/databricks/spark-xml)
    */
  case class Xml(
      path: String,
      options: Option[Map[String, String]]
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
