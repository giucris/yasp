package it.yasp.core.spark.model

/** Dest Sum Type
  */
sealed trait Dest extends Product with Serializable

object Dest {

  /** A Csv destination model
    * @param path:
    *   output path
    * @param partitionBy:
    *   Optional [[Seq]] of column name
    * @param options:
    *   Optional [[Map]] of configuration
    */
  final case class Csv(
      path: String,
      partitionBy: Seq[String] = Seq.empty,
      options: Map[String, String] = Map.empty
  ) extends Dest

  /** A Json destination model
    * @param path:
    *   output path
    * @param partitionBy:
    *   Optional [[Seq]] of column name
    * @param options:
    *   Optional [[Map]] of configuration
    */
  final case class Json(
      path: String,
      partitionBy: Seq[String] = Seq.empty,
      options: Map[String, String] = Map.empty
  ) extends Dest

  /** A Parquet destination Model
    * @param path:
    *   output Path
    * @param partitionBy:
    *   Optional [[Seq]] of column name
    */
  final case class Parquet(
      path: String,
      partitionBy: Seq[String] = Seq.empty,
      options: Map[String, String] = Map.empty
  ) extends Dest

  /** A Orc destination model
    *
    * @param path
    *   : output path
    * @param partitionBy
    *   : Optional [[Seq]] of column name
    * @param options:
    *   Optional [[Map]] of configuration
    */
  final case class Orc(
      path: String,
      partitionBy: Seq[String] = Seq.empty,
      options: Map[String, String] = Map.empty
  ) extends Dest

  /** An Avro destination model
    * @param path
    *   : output Path
    * @param partitionBy
    *   : Optional [[Seq]] of column name
    * @param options
    *   : Optional [[Map]] of configuration
    */
  final case class Avro(
      path: String,
      partitionBy: Seq[String] = Seq.empty,
      options: Map[String, String] = Map.empty
  ) extends Dest

  final case class Xml(
      path: String,
      partitionBy: Seq[String] = Seq.empty,
      options: Map[String, String] = Map.empty
  ) extends Dest

  final case class Jdbc(
      url: String,
      table: String,
      credentials: Option[BasicCredentials],
      truncate: Option[Boolean]
  ) extends Dest

}
