package it.yasp.core.spark.model

/** Process SumType
  */
sealed trait Process extends Product with Serializable

object Process {

  /** Define a Sql Process
    * @param query:
    *   Spark sql query that will be executed
    */
  final case class Sql(query: String) extends Process

  final case class Custom(clazz: String, options: Option[Map[String, String]]) extends Process
}
