package it.yasp.core.spark.plugin

import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait ProcessorPlugin {
  def process(sparkSession: SparkSession, options: Option[Map[String, String]]): Dataset[Row]
}
