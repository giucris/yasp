package it.yasp.core.spark.session

/** A SumType that describe all possible value for a Session.
  *
  * Possible values are: [[SessionType.Local]] or [[SessionType.Distributed]]
  */
sealed trait SessionType

object SessionType {

  /** Represent a Local Spark session, used to execute Spark Session in a local mode
    */
  case object Local extends SessionType

  /** Represent a Distributed Spark session, used to execute Spark on a cluster
    */
  case object Distributed extends SessionType
}
