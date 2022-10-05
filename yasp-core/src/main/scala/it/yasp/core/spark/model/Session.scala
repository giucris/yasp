package it.yasp.core.spark.model

/** Session Product Type
  *
  * @param kind
  *   a [[SessionType]]
  * @param name
  *   : Application name
  * @param conf:
  *   A [[Map]] of spark specific session configuration
  * @param checkPointLocation:
  *   CheckPoint location that will be used by the spark application to store checkpointed dataset
  */
final case class Session(
    kind: SessionType,
    name: String,
    conf: Map[String, String] = Map.empty,
    checkPointLocation: Option[String] = None
)

object Session {

  /** Implicit class for SessionOps
    * @param session:
    *   [[Session]]
    */
  implicit class SessionOps(session: Session) {
    private val LOCAL_MASTER = "local[*]"

    /** Provide an Optional value for master configuration.
      *
      * @return
      *   Some(local[*]) if session is Local None otherwise
      */
    def master: Option[String] =
      session.kind match {
        case SessionType.Local       => Some(LOCAL_MASTER)
        case SessionType.Distributed => None
      }
  }
}
