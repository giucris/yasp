package it.yasp.core.spark.writer

import it.yasp.core.spark.model.{Dest, OutFormat}
import it.yasp.core.spark.writer.Writable.ops._
import org.apache.spark.sql.{Dataset, Row}

/** Writer
  *
  * Provide a write method to store a [[Dataset]] into a specific [[Dest]]
  * @tparam A
  */
trait Writer[A] {
  def write(data: Dataset[Row], dest: A): Unit
}

object Writer {

  def writer[A](implicit ev:Writable[A]): Writer[A] = new Writer[A] {
    override def write(data: Dataset[Row], dest: A): Unit = {
      val format = dest.format
      val writer = data.write.format(format.fmt).options(format.options)
      if (format.partitionBy.isEmpty) writer.save
      else writer.partitionBy(format.partitionBy: _*).save
    }
  }

}
