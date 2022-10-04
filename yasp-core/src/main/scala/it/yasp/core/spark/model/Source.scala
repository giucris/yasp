package it.yasp.core.spark.model

/** Source Sum type
  */
sealed trait Source extends Product with Serializable

object Source {

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

}
