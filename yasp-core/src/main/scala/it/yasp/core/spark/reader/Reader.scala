package it.yasp.core.spark.reader

import com.databricks.spark.xml.XmlDataFrameReader
import it.yasp.core.spark.model.Source
import it.yasp.core.spark.model.Source._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** Reader
  *
  * Provide a read method to load a specific [[Source]]
  *
  * @tparam A:
  *   Source
  */
trait Reader[A <: Source] {

  /** Read a specific datasource with spark primitives
    *
    * @param source
    *   : an instance of [[Source]]
    * @return
    *   a [[Dataset]] of [[Row]]
    */
  def read(source: A): Dataset[Row]

}

object Reader {

  class CsvReader(spark: SparkSession) extends Reader[Csv] {
    override def read(source: Csv): Dataset[Row] =
      spark.read
        .options(Map("header" -> source.header.toString, "sep" -> source.separator))
        .csv(source.paths: _*)
  }

  class ParquetReader(spark: SparkSession) extends Reader[Parquet] {
    override def read(source: Parquet): Dataset[Row] =
      spark.read
        .options(Map("mergeSchema" -> source.mergeSchema.toString))
        .parquet(source.paths: _*)
  }

  class JsonReader(spark: SparkSession) extends Reader[Json] {
    override def read(source: Json): Dataset[Row] =
      spark.read.json(source.paths: _*)
  }

  class JDBCReader(spark: SparkSession) extends Reader[Jdbc] {
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

  class AvroReader(spark: SparkSession) extends Reader[Avro] {
    override def read(source: Avro): Dataset[Row] =
      spark.read.format("avro").load(source.paths.mkString(","))
  }

  class XmlReader(spark: SparkSession) extends Reader[Xml] {
    override def read(source: Xml): Dataset[Row] =
      spark.read.option("rowTag", source.rowTag).xml(source.paths.mkString(","))
  }

  class SourceReader(spark: SparkSession) extends Reader[Source] {

    override def read(source: Source): Dataset[Row] =
      source match {
        case s @ Source.Csv(_, _, _)  => new CsvReader(spark).read(s)
        case s @ Source.Parquet(_, _) => new ParquetReader(spark).read(s)
        case s @ Source.Json(_)       => new JsonReader(spark).read(s)
        case s @ Source.Avro(_)       => new AvroReader(spark).read(s)
        case s @ Source.Xml(_, _)     => new XmlReader(spark).read(s)
        case s @ Source.Jdbc(_, _, _) => new JDBCReader(spark).read(s)
      }
  }

}
