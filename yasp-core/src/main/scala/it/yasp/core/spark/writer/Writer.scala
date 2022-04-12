package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest.Parquet
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

  /** ParquetWriter an implementation of Writer[Parquet]
    */
  class ParquetWriter extends Writer[Parquet] {
    override def write(dataFrame: DataFrame, dest: Parquet): Unit = {
      val writer = dest.mode.map(dataFrame.write.mode).getOrElse(dataFrame.write).format("parquet")
      if (dest.partitionBy.isEmpty) writer.save(dest.path)
      else writer.partitionBy(dest.partitionBy: _*).save(dest.path)
    }
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
      }
  }
}
