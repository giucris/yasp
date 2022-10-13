package it.yasp.core.spark.model

/** Session Product Type
  *
  * @param kind
  *   a [[SessionType]]
  * @param name
  *   : Application name
  * @param conf:
  *   A [[Map]] of spark specific session configuration
  * @param withHiveSupport:
  *   An optional [[Boolean]] for enabling hive support
  * @param withCheckpointDir:
  *   CheckPoint location that will be used by the spark application to store checkpointed dataset
  */
final case class Session(
    kind: SessionType,
    name: String,
    conf: Option[Map[String, String]] = None,
    withHiveSupport: Option[Boolean] = None,
    withDeltaSupport: Option[Boolean] = None,
    withIcebergSupport: Option[Boolean] = None,
    withCheckpointDir: Option[String] = None
)

object Session {

  /** SessionOps
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
