package it.yasp.core.spark.writer

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.WriteError
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
    * @return
    *   Right() if write action works fine Left(WriteError) if something goes wrong during writing
    *   data
    */
  def write(dataFrame: DataFrame, dest: A): Either[WriteError, Unit]
}

object Writer {

  class FormatWriter extends Writer[Format] with StrictLogging {
    override def write(dataFrame: DataFrame, dest: Format): Either[WriteError, Unit] = {
      logger.info(s"Write Format: $dest")
      try {
        val wr1 = dataFrame.write.format(dest.format).options(dest.options)
        val wr2 = dest.mode.map(wr1.mode).getOrElse(wr1)
        val wr3 = if (dest.partitionBy.isEmpty) wr2 else wr2.partitionBy(dest.partitionBy: _*)
        wr3.save()
        Right(())
      } catch { case t: Throwable => Left(WriteError(dest, t)) }
    }
  }

  class DestWriter extends Writer[Dest] {
    override def write(dataFrame: DataFrame, dest: Dest): Either[WriteError, Unit] =
      dest match {
        case d: Format => new FormatWriter().write(dataFrame, d)
      }
  }
}
