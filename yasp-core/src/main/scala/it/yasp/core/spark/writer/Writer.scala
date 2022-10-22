package it.yasp.core.spark.writer

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.WriteError
import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest.{Custom, Format, HiveTable}
import it.yasp.core.spark.plugin.{PluginProvider, WriterPlugin}
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
    *   Right() if write action works fine Left(WriteError) if something goes wrong during writing data
    */
  def write(dataFrame: DataFrame, dest: A): Either[WriteError, Unit]
}

object Writer {

  class FormatWriter extends Writer[Format] with StrictLogging {
    override def write(dataFrame: DataFrame, dest: Format): Either[WriteError, Unit] = {
      logger.info(s"Write Format: $dest")
      try {
        val wr1 = dataFrame.write.options(dest.options)
        val wr2 = dest.mode.fold(wr1)(wr1.mode)
        val wr3 = Option(dest.partitionBy).filter(_.nonEmpty).fold(wr2)(wr2.partitionBy(_: _*))
        wr3.format(dest.format).save()
        Right(())
      } catch { case t: Throwable => Left(WriteError(dest, t)) }
    }
  }

  class HiveTableWriter extends Writer[HiveTable] with StrictLogging {
    override def write(dataFrame: DataFrame, dest: HiveTable): Either[WriteError, Unit] = {
      logger.info(s"Write HiveTable: $dest")
      try {
        val wr1 = dataFrame.write.options(dest.options)
        val wr2 = dest.mode.fold(wr1)(wr1.mode)
        val wr3 = Option(dest.partitionBy).filter(_.nonEmpty).fold(wr2)(wr2.partitionBy(_: _*))
        wr3.saveAsTable(dest.table)
        Right(())
      } catch {
        case t: Throwable => Left(WriteError(dest, t))
      }
    }
  }

  class CustomWriter(pluginProvider: PluginProvider) extends Writer[Custom] with StrictLogging {
    override def write(dataFrame: DataFrame, dest: Custom): Either[WriteError, Unit] = {
      logger.info(s"Writing custom dest: $dest")
      pluginProvider
        .load[WriterPlugin](dest.clazz)
        .flatMap { writer =>
          try Right(writer.write(dataFrame, dest.options))
          catch { case t: Throwable => Left(WriteError(dest, t)) }
        }
        .leftMap(WriteError(dest, _))
    }
  }

  class DestWriter extends Writer[Dest] {
    override def write(dataFrame: DataFrame, dest: Dest): Either[WriteError, Unit] =
      dest match {
        case d: Format    => new FormatWriter().write(dataFrame, d)
        case d: HiveTable => new HiveTableWriter().write(dataFrame, d)
        case d: Custom    => new CustomWriter(new PluginProvider()).write(dataFrame, d)
      }
  }
}
