package it.yasp.core.spark.reader

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.ReadError
import it.yasp.core.spark.model.Source
import it.yasp.core.spark.model.Source._
import it.yasp.core.spark.plugin.ReaderPlugin
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util.ServiceLoader

/** Reader
  *
  * Provide a read method to load a specific [[Source]]
  *
  * @tparam A:
  *   Source
  */
trait Reader[A <: Source] {

  /** Read a specific datasource with spark primitives
    *
    * @param source
    *   : an instance of [[Source]]
    * @return
    *   a [[Dataset]] of [[Row]]
    */
  def read(source: A): Either[ReadError, Dataset[Row]]

}

object Reader {

  import scala.jdk.CollectionConverters._

  class CustomReader(spark: SparkSession) extends Reader[Custom] with StrictLogging {
    override def read(source: Custom): Either[ReadError, Dataset[Row]] = {
      logger.info(s"Loading class: ${source.clazz}")
      loadReaderInstance(source).flatMap { reader =>
        try Right(reader.read(spark, source.options))
        catch { case t: Throwable => Left(ReadError(source, t)) }
      }
    }

    private def loadReaderInstance(source: Custom): Either[ReadError, ReaderPlugin] =
      try ServiceLoader
        .load(classOf[ReaderPlugin])
        .iterator()
        .asScala
        .toSeq
        .headOption
        .toRight(ReadError(source, new ClassNotFoundException(source.clazz)))
      catch { case t: Throwable => Left(ReadError(source, t)) }
  }

  /** A FormatReader. Will use the standard spark approach to read a dataset starting from a configured format
    * @param spark:
    *   [[SparkSession]] that will be used to read the Format Source
    */
  class FormatReader(spark: SparkSession) extends Reader[Format] with StrictLogging {
    override def read(source: Format): Either[ReadError, Dataset[Row]] = {
      logger.info(s"Reading Format: $source")
      try Right {
        source.schema
          .fold(spark.read)(spark.read.schema)
          .format(source.format)
          .options(source.options)
          .load()
      } catch { case t: Throwable => Left(ReadError(source, t)) }
    }
  }

  /** A HiveTableReader. Will use the standard spark.table to retrieve an hive table
    * @param spark:
    *   [[SparkSession]] that will be used to read the Format Source
    */
  class HiveTableReader(spark: SparkSession) extends Reader[HiveTable] with StrictLogging {
    override def read(source: HiveTable): Either[ReadError, Dataset[Row]] =
      try Right(spark.read.table(source.table))
      catch { case t: Throwable => Left(ReadError(source, t)) }
  }

  /** SourceReader an instance of Reader[Source] Provide a method to dispatch the specific source to the specific method
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class SourceReader(spark: SparkSession) extends Reader[Source] {
    override def read(source: Source): Either[ReadError, Dataset[Row]] =
      source match {
        case s: Custom    => new CustomReader(spark).read(s)
        case s: Format    => new FormatReader(spark).read(s)
        case s: HiveTable => new HiveTableReader(spark).read(s)
      }
  }

}
