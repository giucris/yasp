package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest.Format
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

  class FormatWriter extends Writer[Format] {
    override def write(dataFrame: DataFrame, dest: Format): Unit = {
      val wr1 = dataFrame.write.format(dest.format).options(dest.options)
      val wr2 = dest.mode.map(wr1.mode).getOrElse(wr1)
      val wr3 = if (dest.partitionBy.isEmpty) wr2 else wr2.partitionBy(dest.partitionBy: _*)
      wr3.save()
    }
  }

  class DestWriter extends Writer[Dest] {
    override def write(dataFrame: DataFrame, dest: Dest): Unit =
      dest match {
        case d: Format => new FormatWriter().write(dataFrame, d)
      }
  }
}
