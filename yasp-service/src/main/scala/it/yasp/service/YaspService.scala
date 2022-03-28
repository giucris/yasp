package it.yasp.service

import it.yasp.core.spark.session.SparkSessionFactory
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
    new DefaultYaspService(new SparkSessionFactory, new YaspExecutorFactory)

  /** YaspService Default implementation
    * @param sessionFactory:
    *   An [[SparkSessionFactory]] instance to generate the [[org.apache.spark.sql.SparkSession]] at
    *   runtime
    * @param yaspExecutorFactory:
    *   An instance of [[YaspExecutorFactory]] to generate a
    *   [[it.yasp.service.executor.YaspExecutor]] at runtime and execute the
    *   [[it.yasp.service.model.YaspPlan]]
    */
  class DefaultYaspService(
      sessionFactory: SparkSessionFactory,
      yaspExecutorFactory: YaspExecutorFactory
  ) extends YaspService {

    override def run(yaspExecution: YaspExecution): Unit = {
      val session = sessionFactory.create(yaspExecution.conf)
      yaspExecutorFactory.create(session).exec(yaspExecution.plan)
    }

  }

}
