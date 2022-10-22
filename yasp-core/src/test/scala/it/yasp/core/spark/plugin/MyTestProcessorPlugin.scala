package it.yasp.core.spark.plugin
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class MyTestProcessorPlugin extends ProcessorPlugin {

  override def process(sparkSession: SparkSession, options: Option[Map[String, String]]): Dataset[Row] =
    sparkSession.emptyDataFrame
}
