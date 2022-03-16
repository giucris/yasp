package it.yasp.core.spark.model

sealed trait Dest

object Dest {
  final case class Parquet(path:String) extends Dest
}
