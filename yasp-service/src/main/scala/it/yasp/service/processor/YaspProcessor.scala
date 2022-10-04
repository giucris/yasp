package it.yasp.service.processor

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError.{CacheOperationError, RepartitionOperationError}
import it.yasp.core.spark.model.Process
import it.yasp.core.spark.operators.Operators
import it.yasp.core.spark.processor.Processor
import it.yasp.core.spark.registry.Registry
import it.yasp.service.err.YaspServiceError.YaspProcessError
import it.yasp.service.model.YaspProcess
import org.apache.spark.sql.{Dataset, Row}

/** YaspProcessor
  *
  * Provide a unified way to execute a [[YaspProcess]]
  */
trait YaspProcessor {

  /** Execute the provided [[YaspProcess]] cache the result if some cache option are specified on
    * the process and register the result as a table.
    * @param process:
    *   A [[YaspProcess]] instance
    */
  def process(process: YaspProcess): Either[YaspProcessError, Unit]
}

object YaspProcessor {

  /** DefaultYaspProcessor implementation
    *
    * @param processor
    *   : A [[Processor]] instance
    * @param operators
    *   : A [[Registry]] instance
    * @param registry
    *   : A [[Operators]] instance
    */
  class DefaultYaspProcessor(
      processor: Processor[Process],
      operators: Operators,
      registry: Registry
  ) extends YaspProcessor
      with StrictLogging {
    override def process(process: YaspProcess): Either[YaspProcessError, Unit] = {
      logger.info(s"Process: $process")
      for {
        ds1 <- processor.execute(process.process).leftMap(e => YaspProcessError(process, e))
        ds2 <- process.partitions
                 .map(operators.repartition(ds1, _))
                 .fold[Either[RepartitionOperationError, Dataset[Row]]](Right(ds1))(f => f)
                 .leftMap(e => YaspProcessError(process, e))
        ds3 <- process.cache
                 .map(operators.cache(ds2, _))
                 .fold[Either[CacheOperationError, Dataset[Row]]](Right(ds2))(f => f)
                 .leftMap(e => YaspProcessError(process, e))

        _ <- registry.register(ds3, process.id).leftMap(e => YaspProcessError(process, e))

      } yield ()
    }
  }
}
