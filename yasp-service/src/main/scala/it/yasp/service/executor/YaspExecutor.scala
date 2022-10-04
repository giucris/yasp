package it.yasp.service.executor

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.service.err.YaspServiceError
import it.yasp.service.loader.YaspLoader
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
    * Load all [[it.yasp.service.model.YaspSource]], execute all
    * [[it.yasp.service.model.YaspProcess]] and write all [[it.yasp.service.model.YaspSink]]
    * @param yaspPlan:
    *   a [[YaspPlan]] instance to execute
    */
  def exec(yaspPlan: YaspPlan): Either[YaspServiceError, Unit]
}

object YaspExecutor {

  def apply(loader: YaspLoader, processor: YaspProcessor, writer: YaspWriter): YaspExecutor =
    new DefaultYaspExecutor(loader, processor, writer)

  /** A YaspExecutor default implementation
    * @param loader:
    *   A [[YaspLoader]] instance that will load the [[it.yasp.service.model.YaspSource]]
    * @param processor:
    *   A [[YaspProcessor]] instance that will load the [[it.yasp.service.model.YaspProcess]]
    * @param writer:
    *   A [[YaspWriter]] instance that will write the [[it.yasp.service.model.YaspSink]]
    */
  class DefaultYaspExecutor(
      loader: YaspLoader,
      processor: YaspProcessor,
      writer: YaspWriter
  ) extends YaspExecutor
      with StrictLogging {

    override def exec(yaspPlan: YaspPlan): Either[YaspServiceError, Unit] = {
      logger.info(s"Execute Yasp plan: $yaspPlan")
      for {
        _ <- yaspPlan.sources.toList.traverse(loader.load)
        _ <- yaspPlan.processes.toList.traverse(processor.process)
        _ <- yaspPlan.sinks.toList.traverse(writer.write)
      } yield ()
    }

  }

}
