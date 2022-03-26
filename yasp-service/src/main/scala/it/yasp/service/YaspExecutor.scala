package it.yasp.service

import it.yasp.core.spark.session.SparkSessionFactory
import it.yasp.service.model.YaspExecution

trait YaspExecutor {
  def exec(yaspExecution: YaspExecution)
}

object YaspExecutor {

  class DefaultYaspExecutor(
      sessionFactory: SparkSessionFactory,
      yaspServiceFactory: YaspServiceFactory
  ) extends YaspExecutor {

    override def exec(yaspExecution: YaspExecution): Unit = {
      val session = sessionFactory.create(yaspExecution.conf)
      yaspServiceFactory.create(session).run(yaspExecution.plan)
    }

  }

}
