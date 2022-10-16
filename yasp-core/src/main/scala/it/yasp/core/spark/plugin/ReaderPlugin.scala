package it.yasp.core.spark.plugin

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** ReaderPlugin
  */
trait ReaderPlugin {

  /** Read method.
    * @param sparkSession:
    *   A [[SparkSession]] instance
    * @param options:
    *   Optional Map[String,String]
    * @return
    *   a [[Dataset]]
    */
  def read(sparkSession: SparkSession, options: Option[Map[String, String]]): Dataset[Row]
}
