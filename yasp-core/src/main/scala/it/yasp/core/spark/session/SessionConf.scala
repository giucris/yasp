package it.yasp.core.spark.session

/** SessionConf Product Type
  * @param sessionType
  *   a [[SessionType]]
  * @param appName:
  *   Application name
  * @param config:
  *   A [[Map]] of spark specific session configuration
  */
case class SessionConf(
    sessionType: SessionType,
    appName: String,
    config: Map[String, String]
)
