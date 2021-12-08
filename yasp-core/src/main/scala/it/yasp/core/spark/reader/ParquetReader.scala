package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Parquet
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ParquetReader(spark: SparkSession) {

  def read(parquet: Parquet): Dataset[Row] =
    spark.read.option("mergeSchema", parquet.mergeSchema).parquet(parquet.path)
}
