package it.yasp.core.spark.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ParquetReader(spark: SparkSession) {

  def read(path: String): Dataset[Row] =
    spark.read.parquet(path)
}
