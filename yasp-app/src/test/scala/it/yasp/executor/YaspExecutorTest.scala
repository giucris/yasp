package it.yasp.executor

import it.yasp.core.spark.model.{Dest, Source}
import it.yasp.core.spark.session.SessionType.Local
import it.yasp.core.spark.session.{SessionConf, SparkSessionFactory}
import it.yasp.model.{YaspExecution, YaspPlan, YaspSink, YaspSource}
import it.yasp.service.YaspServiceFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspExecutorTest extends AnyFunSuite with MockFactory {

  test("exec") {
    val sparkSessionFactory = mock[SparkSessionFactory]
    val yaspServiceFactory  = mock[YaspServiceFactory]

    new DefaultYaspExecutor(sparkSessionFactory, yaspServiceFactory).exec(
      YaspExecution(
        sessionConf = SessionConf(Local, "my-app-name", Map.empty),
        yaspPlan = YaspPlan(
          sources = Seq(YaspSource("id", Source.Parquet(Seq("xyz"), mergeSchema = false))),
          processes = Seq.empty,
          sinks = Seq(YaspSink("id", Dest.Parquet("my-path")))
        )
      )
    )
  }

  trait YaspExecutor {
    def exec(yaspExecution: YaspExecution)
  }

  class DefaultYaspExecutor(
      sessionFactory: SparkSessionFactory,
      yaspServiceFactory: YaspServiceFactory
  ) extends YaspExecutor {
    override def exec(yaspExecution: YaspExecution): Unit = {
      val session = sessionFactory.create(yaspExecution.sessionConf)
      yaspServiceFactory.create(session).run(yaspExecution.yaspPlan)
    }
  }
}
