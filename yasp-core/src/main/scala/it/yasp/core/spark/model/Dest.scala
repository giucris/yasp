package it.yasp.core.spark.model

/** Dest Sum Type
  */
sealed trait Dest extends Product with Serializable

object Dest {

  /** A Csv destination Model
    * @param csv:
    *   output path
    * @param options:
    *   csv output options
    * @param partitionBy:
    *   Sequence of column name
    */
  final case class Csv(
      csv: String,
      options: Map[String, String] = Map.empty,
      mode: Option[String] = None,
      partitionBy: Seq[String] = Seq.empty
  ) extends Dest

  /** A Parquet destination Model
    * @param parquet:
    *   output Path
    * @param partitionBy:
    *   Sequence of column name
    */
  final case class Parquet(
      parquet: String,
      mode: Option[String] = None,
      partitionBy: Seq[String] = Seq.empty
  ) extends Dest

  /**
    * A JDBC Destination model
    * @param jdbcUrl: Jdbc url
    * @param jdbcAuth: Jdbc auth
    * @param options: options map
    */
  final case class Jdbc(
      jdbcUrl: String,
      jdbcAuth: Option[BasicCredentials] = None,
      options: Map[String, String] = Map.empty
  ) extends Dest
}
