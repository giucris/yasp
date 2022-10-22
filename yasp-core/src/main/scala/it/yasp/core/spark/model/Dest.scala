package it.yasp.core.spark.model

/** Dest Sum Type
  */
sealed trait Dest extends Product with Serializable

object Dest {

  /** Format Dest. Mainly based on the spark format writer. For more docs please check the spark relative documentation.
    *
    * @param format:
    *   Kind of format
    * @param options:
    *   Options configuration
    * @param mode:
    *   SaveMode
    * @param partitionBy:
    *   Seq of columns to partition the output
    */
  final case class Format(
      format: String,
      options: Map[String, String],
      mode: Option[String] = None,
      partitionBy: Seq[String] = Seq.empty
  ) extends Dest

  /** HiveTable Dest. Mainly based on the spark saveAsTable writer functionality.
    * @param table:
    *   table name
    * @param options:
    *   Options configuration
    * @param mode:
    *   SaveMode
    * @param partitionBy:
    *   Seq of columns to partition the output
    */
  final case class HiveTable(
      table: String,
      options: Map[String, String] = Map.empty,
      mode: Option[String] = None,
      partitionBy: Seq[String] = Seq.empty
  ) extends Dest

  final case class Custom(
      clazz: String,
      options: Option[Map[String, String]]
  ) extends Dest
}
