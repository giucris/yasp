package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest.{Csv, Parquet}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  /** ParquetWriter an implementation of Writer[Parquet]
    */
  class ParquetWriter extends Writer[Parquet] with SparkWriterSupport {
    override def write(dataFrame: DataFrame, dest: Parquet): Unit =
      write(dataFrame, "parquet", dest.options ++ Map("path" -> dest.path), dest.partitionBy)
  }

  //TODO Something that retrieve automatically the relative Writer[A] should be implemented. Instead of doing it with an exhaustive pattern matching. probably shapeless could help on this
  /** DestWriter an implementation of Writer[Dest]
    *
    * Provide a method to dispatch the write request at the specific Writer implementation A
    * [[SparkSession]] instance
    */
  class DestWriter extends Writer[Dest] {
    override def write(dataFrame: DataFrame, dest: Dest): Unit =
      dest match {
        case d @ Parquet(_, _, _) => new ParquetWriter().write(dataFrame, d)
      }
  }
}
