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
