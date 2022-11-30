package it.yasp.service.loader

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError
import it.yasp.core.spark.model.Source
import it.yasp.core.spark.operators.DataOperators
import it.yasp.core.spark.reader.Reader
import it.yasp.core.spark.registry.Registry
import it.yasp.service.err.YaspServiceError.YaspLoaderError
import it.yasp.service.model.YaspSource
import org.apache.spark.sql.{Dataset, Row}

/** YaspLoader
  *
  * Provide a unified way to load a [[YaspSource]]
  */
trait YaspLoader {

  /** Read the specified YaspSource, cache the result if some cache specification exists on the source and then register
    * the table
    * @param source:
    *   A [[YaspSource]] instance
    */
  def load(source: YaspSource): Either[YaspLoaderError, Unit]
}

object YaspLoader {

  /** DefaultYaspLoader Implementation
    *
    * @param reader
    *   : A [[Reader]] instance
    * @param registry
    *   : A [[Registry]] instance
    * @param operators
    *   : A [[DataOperators]] instance
    */
  class DefaultYaspLoader(
      reader: Reader[Source],
      operators: DataOperators,
      registry: Registry
  ) extends YaspLoader
      with StrictLogging {
    override def load(source: YaspSource): Either[YaspLoaderError, Unit] = {
      logger.info(s"Source: $source")
      for {
        ds1 <- reader.read(source.source).leftMap(e => YaspLoaderError(source, e))
        ds2 <- source.dataOps
                 .map(operators.exec(ds1, _))
                 .fold[Either[YaspCoreError, Dataset[Row]]](Right(ds1))(f => f)
                 .leftMap(e => YaspLoaderError(source, e))
        _   <- registry.register(ds2, source.id).leftMap(e => YaspLoaderError(source, e))
      } yield ()
    }
  }
}
