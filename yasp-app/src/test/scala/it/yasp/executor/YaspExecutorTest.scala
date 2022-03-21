package it.yasp.executor

import it.yasp.core.spark.model.{Dest, Source}
import it.yasp.core.spark.session.{SessionConf, SparkSessionFactory}
import it.yasp.core.spark.session.SessionType.Local
import it.yasp.model.{YaspExecution, YaspPlan, YaspSink, YaspSource}
import it.yasp.service.YaspService
import it.yasp.service.YaspService.DefaultYaspService
import org.scalatest.funsuite.AnyFunSuite

class YaspExecutorTest extends AnyFunSuite {

  test("exec"){
    new DefaultYaspExecutor(new SparkSessionFactory()).exec(
      YaspExecution(
        sessionConf = SessionConf(Local,"my-app-name",Map.empty),
        yaspPlan = YaspPlan(
          sources = Seq(YaspSource("id",Source.Parquet(Seq("xyz"),mergeSchema = false))),
          processes = Seq.empty,
          sinks = Seq(YaspSink("id",Dest.Parquet("my-path")))
        )
      )
    )
  }

  trait YaspExecutor{
    def exec(yaspExecution: YaspExecution)
  }

  class DefaultYaspExecutor(sessionFactory: SparkSessionFactory,yaspService: YaspService) extends YaspExecutor {
    override def exec(yaspExecution: YaspExecution): Unit = {
      val session = sessionFactory.create(yaspExecution.sessionConf)
      yaspService.run(yaspExecution.yaspPlan)
    }
  }
}
