package it.yasp.core.spark.reader

import it.yasp.core.spark.model.DataSource.{Csv, Parquet}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait DataSourceReader[A] {
  def read(source: A): Dataset[Row]
}

object DataSourceReader {

  class CsvDataSourceReader(spark: SparkSession) extends DataSourceReader[Csv] {
    override def read(source: Csv): Dataset[Row] =
      spark.read
        .option("header", source.header)
        .option("sep", source.separator)
        .csv(source.paths: _*)
  }

  class ParquetDataSourceReader(spark: SparkSession) extends DataSourceReader[Parquet] {
    override def read(source: Parquet): Dataset[Row] =
      spark.read
        .option("mergeSchema", source.mergeSchema)
        .parquet(source.paths: _*)
  }
}
