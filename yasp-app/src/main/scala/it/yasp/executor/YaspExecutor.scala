package it.yasp.executor

import it.yasp.core.spark.session.SparkSessionFactory
import it.yasp.model.YaspExecution
import it.yasp.service.YaspServiceFactory

trait YaspExecutor {
  def exec(yaspExecution: YaspExecution)
}

object YaspExecutor {

  class DefaultYaspExecutor(
      sessionFactory: SparkSessionFactory,
      yaspServiceFactory: YaspServiceFactory
  ) extends YaspExecutor {

    override def exec(yaspExecution: YaspExecution): Unit = {
      val session = sessionFactory.create(yaspExecution.sessionConf)
      yaspServiceFactory.create(session).run(yaspExecution.plan)
    }

  }

}
