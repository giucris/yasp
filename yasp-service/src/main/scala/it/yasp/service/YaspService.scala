package it.yasp.service

import com.typesafe.scalalogging.StrictLogging
import it.yasp.core.spark.factory.SessionFactory
import it.yasp.service.executor.YaspExecutorFactory
import it.yasp.service.model.YaspExecution

/** YaspService
  *
  * Provide a method to run a [[YaspExecution]]
  */
trait YaspService {

  /** Run a [[YaspExecution]]
    *
    * Create the [[org.apache.spark.sql.SparkSession]], load all
    * [[it.yasp.service.model.YaspSource]], execute all [[it.yasp.service.model.YaspProcess]] and
    * write all [[it.yasp.service.model.YaspSink]] Execute all processes
    * @param yaspExecution:
    *   A [[it.yasp.service.model.YaspExecution]] instance to run
    */
  def run(yaspExecution: YaspExecution)
}

object YaspService {

  def apply(): YaspService =
    new DefaultYaspService(new SessionFactory, new YaspExecutorFactory)

  /** YaspService Default implementation
    *
    * @param sessionFactory
    *   : An [[SessionFactory]] instance to generate the [[org.apache.spark.sql.SparkSession]] at
    *   runtime
    * @param yaspExecutorFactory:
    *   An instance of [[YaspExecutorFactory]] to generate a
    *   [[it.yasp.service.executor.YaspExecutor]] at runtime and execute the
    *   [[it.yasp.service.model.YaspPlan]]
    */
  class DefaultYaspService(
      sessionFactory: SessionFactory,
      yaspExecutorFactory: YaspExecutorFactory
  ) extends YaspService with StrictLogging {

    override def run(yaspExecution: YaspExecution): Unit = {
      logger.info(s"Execute YaspService with YaspExecution: $yaspExecution")
      val session = sessionFactory.create(yaspExecution.session)
      yaspExecutorFactory.create(session).exec(yaspExecution.plan)
    }

  }

}
