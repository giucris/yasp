package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest.Parquet
import org.apache.spark.sql.DataFrame

trait Writer[A <: Dest] {
  def write(dataFrame: DataFrame, dest: A): Unit
}

object Writer {
  class ParquetWriter extends Writer[Parquet] {
    override def write(dataFrame: DataFrame, dest: Parquet): Unit =
      dataFrame.write.parquet(dest.path)
  }
}
