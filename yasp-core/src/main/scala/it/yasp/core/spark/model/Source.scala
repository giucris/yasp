package it.yasp.core.spark.model

/** Source Sumtype
  */
sealed trait Source extends Product with Serializable

object Source {

  /** A Csv Source Model
    * @param csv:
    *   path of the Csv files
    * @param options:
    *   An optional map of Csv read configuration. For a complete list of valid configuration check
    *   the official spark documentation
    *   (https://spark.apache.org/docs/2.4.7/api/java/org/apache/spark/sql/DataFrameReader.html#csv-java.lang.String...-).
    */
  final case class Csv(
      csv: String,
      schema: Option[String] = None,
      options: Map[String, String] = Map.empty
  ) extends Source

  /** A Json Source Model
    * @param json:
    *   path of the json files
    * @param schema:
    *   string schema
    * @param options:
    *   An optional map of Json read configuration. For a complete list of valid configuration check
    *   the official spark documentation
    *   (https://spark.apache.org/docs/2.4.7/api/java/org/apache/spark/sql/DataFrameReader.html#json-java.lang.String...-).
    */
  final case class Json(
      json: String,
      schema: Option[String] = None,
      options: Map[String, String] = Map.empty
  ) extends Source

  /** A Parquet Source Model
    * @param parquet:
    *   path of the Parquet files
    * @param mergeSchema:
    *   option that enable the merge schema operation on read
    */
  final case class Parquet(
      parquet: String,
      mergeSchema: Option[Boolean] = None
  ) extends Source

  /** An Orc Source Model
    * @param orc:
    *   path of the Orc files
    */
  final case class Orc(
      orc: String
  ) extends Source

  /** An Avro Source Model
    * @param avro:
    *   path of the avro files
    * @param options:
    *   An optional map of Avro read configuration. For a complete list of valid configuration check
    *   the official spark documentation
    *   (https://spark.apache.org/docs/2.4.7/sql-data-sources-avro.html)
    */
  final case class Avro(
      avro: String,
      options: Map[String, String] = Map.empty
  ) extends Source

  /** An Xml Source Model
    * @param xml:
    *   path of the xml files
    * @param options:
    *   An optional map of Xml read configuration. For a complete list of valid configuration check
    *   the official databricks spark-xml github repository.
    *   (https://github.com/databricks/spark-xml)
    */
  final case class Xml(
      xml: String,
      options: Map[String, String] = Map.empty
  ) extends Source

  /** A Jdbc Source Model
    * @param jdbcUrl:
    *   the url of the database
    * @param jdbcAuth:
    *   an Optional BasicCredential configuration
    * @param options:
    *   An optional map of Jdbc read configuration. For a complete list of valid configuration check
    *   the official spark documentation
    *   (https://spark.apache.org/docs/2.4.7/sql-data-sources-jdbc.html)
    */
  final case class Jdbc(
      jdbcUrl: String,
      jdbcAuth: Option[BasicCredentials] = None,
      options: Map[String, String] = Map.empty
  ) extends Source

}
