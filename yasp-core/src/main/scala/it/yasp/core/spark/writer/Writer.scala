package it.yasp.core.spark.writer

import it.yasp.core.spark.writer.Writable.ops._
import org.apache.spark.sql.{Dataset, Row}

/** Writer
  *
  * Provide a write method to store a [[Dataset]] into a dest [[A]]
  * @tparam A:
  *   Dest type
  */
trait Writer[A] {

  /** Write the provided [[Dataset]] into a destination of type [[A]]
    * @param data:
    *   a [[Dataset]] of [[Row]]
    * @param dest:
    *   a destination of type [[A]]
    */
  def write(data: Dataset[Row], dest: A): Unit
}

object Writer {

  /** Writer method that create a [[Writer]] intance of type [[A]]
    * @param ev:
    *   implicit [[Writable]] for type [[A]]
    * @tparam A:
    *   destination type
    * @return
    *   a [[Writer]] instance of [[A]]
    */
  def writer[A](implicit ev: Writable[A]): Writer[A] = new Writer[A] {
    override def write(data: Dataset[Row], dest: A): Unit = {
      val format = dest.format
      val writer = data.write.format(format.fmt).options(format.options)
      if (format.partitionBy.isEmpty) writer.save
      else writer.partitionBy(format.partitionBy: _*).save
    }
  }

}
