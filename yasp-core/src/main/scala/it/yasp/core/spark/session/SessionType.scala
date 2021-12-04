package it.yasp.core.spark.session

/** A SumType that describe all possible value for a Session.
  *
  * Possible values are: [[SessionType.Local]] or [[SessionType.Distributed]]
  */
sealed trait SessionType

object SessionType {

  /** Local session
    */
  case object Local extends SessionType

  /** Distributed session
    */
  case object Distributed extends SessionType
}
