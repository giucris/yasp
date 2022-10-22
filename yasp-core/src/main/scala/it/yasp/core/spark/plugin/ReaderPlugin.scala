package it.yasp.core.spark.plugin

import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait ReaderPlugin {
  def read(sparkSession: SparkSession, options: Option[Map[String, String]]): Dataset[Row]
}
