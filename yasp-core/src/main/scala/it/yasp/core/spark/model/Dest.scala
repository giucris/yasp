package it.yasp.core.spark.model

sealed trait Dest extends Product with Serializable

object Dest {
  final case class Parquet(path: String, partitionBy: Option[Seq[String]]) extends Dest
}
