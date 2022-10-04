package it.yasp.core.spark.reader

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.ReadError
import it.yasp.core.spark.model.Source
import it.yasp.core.spark.model.Source._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

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

  /** A FormatReader. Will use the standard spark approach to read a dataset starting from a
    * configured format
    * @param spark:
    *   [[SparkSession]] that will be used to read the Format Source
    */
  class FormatReader(spark: SparkSession) extends Reader[Format] with StrictLogging {
    override def read(source: Format): Either[ReadError, Dataset[Row]] = {
      logger.info(s"Reading Format: $source")
      try Right {
        source.schema
          .map(spark.read.schema)
          .getOrElse(spark.read)
          .format(source.format)
          .options(source.options)
          .load()
      } catch { case t: Throwable => Left(ReadError(source, t)) }
    }
  }

  //TODO Something that retrieve automatically the relative Reader[A] should be implemented. Instead of doing it with an exhaustive pattern matching. probably shapeless could help on this
  /** SourceReader an instance of Reader[Source] Provide a method to dispatch the specific source to
    * the specific method
    * @param spark:
    *   A [[SparkSession]] instance
    */
  class SourceReader(spark: SparkSession) extends Reader[Source] {
    override def read(source: Source): Either[ReadError, Dataset[Row]] =
      source match {
        case s: Format => new FormatReader(spark).read(s)
      }
  }

}
