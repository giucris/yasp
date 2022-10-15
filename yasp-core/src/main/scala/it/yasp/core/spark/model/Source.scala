package it.yasp.core.spark.model

/** Source Sumtype
  */
sealed trait Source extends Product with Serializable

object Source {

  final case class Custom(
      clazz: String,
      options: Option[Map[String, String]]
  ) extends Source

  /** A Format Source Model. Mainly based on the spark format reader.
    * @param format:
    *   Kind of format
    * @param schema:
    *   Optional schema
    * @param options:
    *   Map of options
    */
  final case class Format(
      format: String,
      schema: Option[String] = None,
      options: Map[String, String] = Map.empty
  ) extends Source

  /** A HiveTable Source Model. Mainly based on the spark.table reader.
    * @param table:
    *   Table name
    */
  final case class HiveTable(
      table: String
  ) extends Source

}
