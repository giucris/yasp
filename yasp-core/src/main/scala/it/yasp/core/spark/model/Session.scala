package it.yasp.core.spark.model

/** Session Product Type
  *
  * @param kind
  *   a [[SessionType]]
  * @param name
  *   : Application name
  * @param conf:
  *   A [[Map]] of spark specific session configuration
  */
final case class Session(
    kind: SessionType,
    name: String,
    conf: Map[String, String]
)
