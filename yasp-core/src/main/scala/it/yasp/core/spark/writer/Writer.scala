package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest._
import org.apache.spark.sql.DataFrame

/** Writer
  *
  * Provide a write method to store a dataframe into a specific [[Dest]]
  * @tparam A:
  *   type param [[Dest]]
  */
trait Writer[A <: Dest] {

  /** Write a specific [[DataFrame]] to a specific [[Dest]] using spark primitives
    * @param dataFrame:
    *   a [[DataFrame]] instance
    * @param dest:
    *   A [[Dest]] instance
    */
  def write(dataFrame: DataFrame, dest: A): Unit
}

object Writer {

  private[writer] trait SparkWriterSupport {
    def write(
        df: DataFrame,
        format: String,
        options: Map[String, String],
        partitionBy: Seq[String]
    ): Unit = {
      val writer = df.write.format(format).options(options)
      if (partitionBy.isEmpty) writer.save
      else writer.partitionBy(partitionBy: _*).save()
    }

  }

  /** CsvWriter an implementation of Writer[Csv]
    */
  class CsvWriter extends Writer[Csv] with SparkWriterSupport {
    override def write(dataFrame: DataFrame, dest: Csv): Unit =
      write(dataFrame, "csv", dest.options ++ Map("path" -> dest.path), dest.partitionBy)
  }

  class JsonWriter extends Writer[Json] with SparkWriterSupport {
    override def write(dataFrame: DataFrame, dest: Json): Unit =
      write(dataFrame, "json", dest.options ++ Map("path" -> dest.path), dest.partitionBy)
  }

  class AvroWriter extends Writer[Avro] with SparkWriterSupport {
    override def write(dataFrame: DataFrame, dest: Avro): Unit =
      write(dataFrame, "avro", dest.options ++ Map("path" -> dest.path), dest.partitionBy)
  }

  class OrcWriter extends Writer[Orc] with SparkWriterSupport {
    override def write(dataFrame: DataFrame, dest: Orc): Unit =
      write(dataFrame, "orc", dest.options ++ Map("path" -> dest.path), dest.partitionBy)
  }

  class XmlWriter extends Writer[Xml] with SparkWriterSupport {
    override def write(dataFrame: DataFrame, dest: Xml): Unit =
      write(dataFrame, "xml", dest.options ++ Map("path" -> dest.path), dest.partitionBy)
  }

  class JdbcWriter extends Writer[Jdbc] with SparkWriterSupport {
    override def write(dataFrame: DataFrame, dest: Jdbc): Unit = {
      val opts = Seq(
        Some("url"                      -> dest.url),
        Some("table"                    -> dest.table),
        dest.credentials.map("user"     -> _.username),
        dest.credentials.map("password" -> _.password)
      ).flatten.toMap
      write(dataFrame, "jdbc", opts, Seq.empty)
    }
  }

  /** ParquetWriter an implementation of Writer[Parquet]
    */
  class ParquetWriter extends Writer[Parquet] with SparkWriterSupport {
    override def write(dataFrame: DataFrame, dest: Parquet): Unit =
      write(dataFrame, "parquet", dest.options ++ Map("path" -> dest.path), dest.partitionBy)
  }

  //TODO Something that retrieve automatically the relative Writer[A] should be implemented. Instead of doing it with an exhaustive pattern matching. probably shapeless could help on this
  /** DestWriter an implementation of Writer[Dest]
    */
  class DestWriter extends Writer[Dest] {
    override def write(dataFrame: DataFrame, dest: Dest): Unit =
      dest match {
        case d: Csv     => new CsvWriter().write(dataFrame, d)
        case d: Json    => new JsonWriter().write(dataFrame, d)
        case d: Parquet => new ParquetWriter().write(dataFrame, d)
        case d: Orc     => new OrcWriter().write(dataFrame, d)
        case d: Avro    => new AvroWriter().write(dataFrame, d)
        case d: Xml     => new XmlWriter().write(dataFrame, d)
        case d: Jdbc    => new JdbcWriter().write(dataFrame, d)
      }
  }
}
