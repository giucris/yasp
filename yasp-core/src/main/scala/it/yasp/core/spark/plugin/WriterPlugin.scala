package it.yasp.core.spark.plugin

import org.apache.spark.sql.{Dataset, Row}

trait WriterPlugin {
  def write(dataset: Dataset[Row], options: Option[Map[String, String]]): Unit
}
