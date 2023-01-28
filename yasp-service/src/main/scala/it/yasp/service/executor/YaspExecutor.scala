package it.yasp.service.executor

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.service.err.YaspServiceError
import it.yasp.service.loader.YaspLoader
import it.yasp.service.model.YaspAction._
import it.yasp.service.model.YaspPlan
import it.yasp.service.processor.YaspProcessor
import it.yasp.service.writer.YaspWriter

/** YaspExecutor
  *
  * Provide a method to execute a [[YaspPlan]]
  */
trait YaspExecutor {

  /** Execute a specific [[YaspPlan]]
    *
    */
  def exec(yaspPlan: YaspPlan): Either[YaspServiceError, Unit]
}

object YaspExecutor {

  def apply(loader: YaspLoader, processor: YaspProcessor, writer: YaspWriter): YaspExecutor =
    new DefaultYaspExecutor(loader, processor, writer)

  /** A YaspExecutor default implementation
    */
  class DefaultYaspExecutor(
                             loader: YaspLoader,
                             processor: YaspProcessor,
                             writer: YaspWriter
                           ) extends YaspExecutor
    with StrictLogging {

    override def exec(yaspPlan: YaspPlan): Either[YaspServiceError, Unit] = {
      logger.info(s"Execute Yasp plan: $yaspPlan")
      yaspPlan.actions.toList.traverse {
        case x: YaspSource => loader.load(x)
        case x: YaspProcess => processor.process(x)
        case x: YaspSink => writer.write(x)
      }.map(_ => Right(()))
    }

  }

}
