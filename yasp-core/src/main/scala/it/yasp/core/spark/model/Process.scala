package it.yasp.core.spark.model

sealed trait Process

object Process {
  final case class Sql(query: String) extends Process
}
