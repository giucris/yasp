package it.yasp.service

import it.yasp.core.spark.session.SparkSessionFactory
import it.yasp.service.executor.YaspExecutorFactory
import it.yasp.service.model.YaspExecution

trait YaspService {
  def run(yaspExecution: YaspExecution)
}

object YaspService {

  def apply(): YaspService =
    new DefaultYaspService(new SparkSessionFactory, new YaspExecutorFactory)

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
