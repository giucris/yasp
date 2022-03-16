package it.yasp.core.spark.model

sealed trait Process extends Product with Serializable

object Process {
  final case class Sql(query: String) extends Process
}
