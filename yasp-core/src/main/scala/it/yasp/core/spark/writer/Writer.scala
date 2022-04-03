package it.yasp.core.spark.writer

import it.yasp.core.spark.model.{Dest, OutFormat}
import org.apache.spark.sql.{Dataset, Row}

/** Writer
  *
  * Provide a write method to store a dataframe into a specific [[Dest]]
  * @tparam A:
  *   type param [[Dest]]
  */
trait Writer[A <: Dest] {
  def write(data: Dataset[Row], dest: A)(implicit ev: Writable[A]): Unit
}

object Writer {

  def writer:Writer[Dest] = new Writer[Dest]{
    override def write(data: Dataset[Row], dest: Dest)(implicit ev: Writable[Dest]): Unit = {
      val fmt    = ev.format(dest)
      val writer = data.write.format(fmt.format).options(fmt.options)
      if (fmt.partitionBy.isEmpty) writer.save
      else writer.partitionBy(fmt.partitionBy: _*).save
    }
  }

}
