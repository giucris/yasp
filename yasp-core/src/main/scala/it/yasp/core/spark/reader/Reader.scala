package it.yasp.core.spark.reader

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

  private[reader] trait SparkReadSupport {
    def read(
        spark: SparkSession,
        format: String,
        options: Map[String, String],
        schema: Option[String]
    ): Dataset[Row] =
      schema.map(spark.read.schema).getOrElse(spark.read).format(format).options(options).load()
  }

  /** CsvReader an instance of Reader[Csv]
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class CsvReader(spark: SparkSession) extends Reader[Csv] with SparkReadSupport {
    override def read(source: Csv): Dataset[Row] = {
      val opts = source.options ++ Map("path" -> source.csv)
      read(spark, format = "csv", opts.filterKeys(_ != "schema"), opts.get("schema"))
    }
  }

  /** JsonReader an instance of Reader[Json]
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class JsonReader(spark: SparkSession) extends Reader[Json] with SparkReadSupport {
    override def read(source: Json): Dataset[Row] = {
      val opts = source.options ++ Map("path" -> source.json)
      read(spark, format = "json", opts.filterKeys(_ != "schema"), opts.get("schema"))
    }
  }

  /** ParquetReader an instance of Reader[Parquet]
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class ParquetReader(spark: SparkSession) extends Reader[Parquet] with SparkReadSupport {
    override def read(source: Parquet): Dataset[Row] = {
      val opts = Map(
        "path"        -> source.parquet,
        "mergeSchema" -> source.mergeSchema.getOrElse(false).toString
      )
      read(spark, format = "parquet", opts, None)
    }
  }

  /** JdbcReader an instance of Reader[Jdbc]
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class JdbcReader(spark: SparkSession) extends Reader[Jdbc] with SparkReadSupport {
    override def read(source: Jdbc): Dataset[Row] = {
      val opts = source.options ++ Map(
        "url"      -> source.url,
        "user"     -> source.credentials.map(_.username).getOrElse(""),
        "password" -> source.credentials.map(_.password).getOrElse("")
      )
      read(spark, format = "jdbc", opts, None)
    }
  }

  /** AvroReader an instance of Reader[Avro]
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class AvroReader(spark: SparkSession) extends Reader[Avro] with SparkReadSupport {
    override def read(source: Avro): Dataset[Row] = {
      val opts = source.options ++ Map("path" -> source.avro)
      read(spark, format = "avro", opts, None)
    }
  }

  /** XmlReader an instance of Reader[Xml]
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class XmlReader(spark: SparkSession) extends Reader[Xml] with SparkReadSupport {
    override def read(source: Xml): Dataset[Row] = {
      val opts = source.options ++ Map("path" -> source.xml)
      read(spark, format = "xml", opts.filterKeys(_ != "schema"), opts.get("schema"))
    }
  }

  /** OrcReader an instance of Reader[Orc]
    * @param spark
    *   A [[SparkSession]] instance
    */
  class OrcReader(spark: SparkSession) extends Reader[Orc] with SparkReadSupport {
    override def read(source: Orc): Dataset[Row] =
      read(spark, format = "orc", Map("path" -> source.orc), None)
  }

  //TODO Something that retrieve automatically the relative Reader[A] should be implemented. Instead of doing it with an exhaustive pattern matching. probably shapeless could help on this
  /** SourceReader an instance of Reader[Source] Provide a method to dispatch the specific source to
    * the specific method
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class SourceReader(spark: SparkSession) extends Reader[Source] {
    override def read(source: Source): Dataset[Row] =
      source match {
        case s: Csv     => new CsvReader(spark).read(s)
        case s: Parquet => new ParquetReader(spark).read(s)
        case s: Json    => new JsonReader(spark).read(s)
        case s: Avro    => new AvroReader(spark).read(s)
        case s: Xml     => new XmlReader(spark).read(s)
        case s: Jdbc    => new JdbcReader(spark).read(s)
        case s: Orc     => new OrcReader(spark).read(s)
      }
  }

}
