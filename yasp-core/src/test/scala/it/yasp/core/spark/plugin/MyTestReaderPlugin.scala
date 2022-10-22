package it.yasp.core.spark.plugin

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class MyTestReaderPlugin extends ReaderPlugin {
  override def read(sparkSession: SparkSession, options: Option[Map[String, String]]): Dataset[Row] =
    sparkSession.emptyDataFrame

}
