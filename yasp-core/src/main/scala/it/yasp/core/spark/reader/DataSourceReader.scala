package it.yasp.core.spark.reader

import com.databricks.spark.xml.XmlDataFrameReader
import it.yasp.core.spark.model.DataSource
import it.yasp.core.spark.model.DataSource.{Csv, Jdbc, Json, Parquet, Xml}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** DataSourceReader
  *
  * Provide a read method to load a specific [[DataSource]]
  * @tparam A
  */
trait DataSourceReader[A <: DataSource] {

  /** Read a specific datasource with spark primitives
    * @param source:
    *   an instance of [[DataSource]]
    * @return
    *   a [[Dataset]] of [[Row]]
    */
  def read(source: A): Dataset[Row]

}

object DataSourceReader {

  class CsvDataSourceReader(spark: SparkSession) extends DataSourceReader[Csv] {
    override def read(source: Csv): Dataset[Row] =
      spark.read
        .options(Map("header" -> source.header.toString, "sep" -> source.separator))
        .csv(source.paths: _*)
  }

  class ParquetDataSourceReader(spark: SparkSession) extends DataSourceReader[Parquet] {
    override def read(source: Parquet): Dataset[Row] =
      spark.read
        .options(Map("mergeSchema" -> source.mergeSchema.toString))
        .parquet(source.paths: _*)
  }

  class JsonDataSourceReader(spark: SparkSession) extends DataSourceReader[Json] {
    override def read(source: Json): Dataset[Row] =
      spark.read.json(source.paths: _*)
  }

  class JDBCDataSourceReader(spark: SparkSession) extends DataSourceReader[Jdbc] {
    override def read(source: Jdbc): Dataset[Row] =
      spark.read
        .options(
          Map(
            "url"      -> source.url,
            "dbtable"  -> source.table,
            "user"     -> source.credentials.map(_.username).getOrElse(""),
            "password" -> source.credentials.map(_.password).getOrElse("")
          )
        )
        .format("jdbc")
        .load()
  }

  class XmlDataSourceReader(spark: SparkSession) extends DataSourceReader[Xml] {
    override def read(source: Xml): Dataset[Row] =
      spark.read.option("rowTag", source.rowTag).xml(source.paths.mkString(","))
  }


}
