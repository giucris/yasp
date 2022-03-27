package it.yasp.app.conf

import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.{Dest, Source}
import it.yasp.core.spark.session.SessionConf
import it.yasp.core.spark.session.SessionType.Local
import it.yasp.service.model._
import it.yasp.testkit.TestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class YaspExecutionLoaderTest extends AnyFunSuite with BeforeAndAfterAll {
  private val workspace = "yasp-app/src/test/resources/YaspExecutionLoaderTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("load") {
    TestUtils.createFile(
      filePath = s"$workspace/example.yml",
      rows = Seq(
        """conf:
          |  sessionType:
          |    Local: {}
          |  appName: my-app
          |  config: {}
          |plan:
          |  sources:
          |    - id: id1
          |      source:
          |        Csv:
          |          paths:
          |            - path1
          |          header: true
          |          separator: ','
          |    - id: id2
          |      source:
          |        Json:
          |          paths:
          |            - path2
          |  processes:
          |    - id: r1
          |      process:
          |        Sql:
          |          query: my query
          |  sinks:
          |    - id: r1
          |      dest:
          |        Parquet:
          |          path: path3
          |""".stripMargin
      )
    )

    val actual   = YaspExecutionLoader.load(s"$workspace/example.yml")
    val expected = YaspExecution(
      SessionConf(Local, "my-app", Map.empty),
      YaspPlan(
        sources = Seq(
          YaspSource("id1", Source.Csv(Seq("path1"), header = true, ","), None),
          YaspSource("id2", Source.Json(Seq("path2")), None)
        ),
        processes = Seq(
          YaspProcess("r1", Sql("my query"))
        ),
        sinks = Seq(
          YaspSink("r1", Dest.Parquet("path3"))
        )
      )
    )
    assert(actual == expected)
  }
}
