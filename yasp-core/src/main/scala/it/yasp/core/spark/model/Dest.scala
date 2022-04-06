package it.yasp.core.spark.model

/** Dest Sum Type
  */
sealed trait Dest extends Product with Serializable

object Dest {

  /** A Parquet destination Model
    * @param path:
    *   output Path
    * @param partitionBy:
    *   Optional Seq of column name
    */
  final case class Parquet(
      path: String,
      partitionBy: Seq[String] = Seq.empty
  ) extends Dest
}
