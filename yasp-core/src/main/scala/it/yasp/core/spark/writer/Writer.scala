package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest.{Csv, Parquet}
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

  private[writer] trait SparkWriteSupport {
    def writeDf(
        dataFrame: DataFrame,
        format: String,
        options: Map[String, String],
        partitionBy: Seq[String],
        mode: Option[String],
        path: String
    ): Unit = {
      val writer     = dataFrame.write.format(format).options(options)
      val writerMode = mode.map(writer.mode).getOrElse(writer)
      if (partitionBy.isEmpty) writerMode.save(path)
      else writerMode.partitionBy(partitionBy: _*).save(path)
    }
  }

  /** CsvWrite an implementation of Writer[Csv]
    */
  class CsvWriter extends Writer[Csv] with SparkWriteSupport {
    override def write(dataFrame: DataFrame, dest: Csv): Unit =
      writeDf(dataFrame, format = "csv", dest.options, dest.partitionBy, dest.mode, dest.csv)
  }

  /** ParquetWriter an implementation of Writer[Parquet]
    */
  class ParquetWriter extends Writer[Parquet] with SparkWriteSupport {
    override def write(dataFrame: DataFrame, dest: Parquet): Unit =
      writeDf(dataFrame, format = "parquet", Map.empty, dest.partitionBy, dest.mode, dest.parquet)
  }

  //TODO Something that retrieve automatically the relative Writer[A] should be implemented. Instead of doing it with an exhaustive pattern matching. probably shapeless could help on this
  /** DestWriter an implementation of Writer[Dest]
    *
    * Provide a method to dispatch the write request at the specific Writer implementation
    */
  class DestWriter extends Writer[Dest] {
    override def write(dataFrame: DataFrame, dest: Dest): Unit =
      dest match {
        case d: Parquet => new ParquetWriter().write(dataFrame, d)
        case d: Csv     => new CsvWriter().write(dataFrame, d)
      }
  }
}
