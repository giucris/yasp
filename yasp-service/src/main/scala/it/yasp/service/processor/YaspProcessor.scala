package it.yasp.service.processor

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.err.YaspCoreError
import it.yasp.core.spark.model.Process
import it.yasp.core.spark.operators.DataOperators
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

  /** Execute the provided [[YaspProcess]] cache the result if some cache option are specified on the process and
    * register the result as a table.
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
    *   : A [[DataOperators]] instance
    */
  class DefaultYaspProcessor(
      processor: Processor[Process],
      operators: DataOperators,
      registry: Registry
  ) extends YaspProcessor
      with StrictLogging {
    override def process(process: YaspProcess): Either[YaspProcessError, Unit] = {
      logger.info(s"Process: $process")
      for {
        ds1 <- processor.execute(process.process).leftMap(e => YaspProcessError(process, e))
        ds2 <- process.dataOps
                 .map(operators.exec(ds1, _))
                 .fold[Either[YaspCoreError, Dataset[Row]]](Right(ds1))(f => f)
                 .leftMap(e => YaspProcessError(process, e))
        _   <- registry.register(ds2, process.id).leftMap(e => YaspProcessError(process, e))
      } yield ()
    }
  }
}
