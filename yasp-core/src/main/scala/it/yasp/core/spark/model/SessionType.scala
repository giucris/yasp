package it.yasp.core.spark.model

/** A SumType that describe all possible value for a Session.
  *
  * Possible values are: [[SessionType.Local]] or [[SessionType.Distributed]]
  */
sealed trait SessionType extends Product with Serializable

object SessionType {

  /** Represent a Local Spark session, used to execute Spark Session in a local mode
    */
  final case object Local extends SessionType

  /** Represent a Distributed Spark session, used to execute Spark on a cluster
    */
  final case object Distributed extends SessionType
}
