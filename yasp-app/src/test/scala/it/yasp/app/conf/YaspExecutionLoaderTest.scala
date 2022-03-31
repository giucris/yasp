package it.yasp.app.conf

import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.SessionType.Local
import it.yasp.core.spark.model._
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
        """session:
          |  kind: Local
          |  name: my-app
          |  conf: {}
          |plan:
          |  sources:
          |    - id: id1
          |      source:
          |        Csv:
          |          path: path1
          |          options:
          |            header: 'true'
          |            sep: ','
          |    - id: id2
          |      source:
          |        Json:
          |          path: path2
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
      Session(Local, "my-app", Map.empty),
      YaspPlan(
        sources = Seq(
          YaspSource("id1", Source.Csv("path1", Some(Map("header" -> "true", "sep" -> ","))), None),
          YaspSource("id2", Source.Json("path2", None), None)
        ),
        processes = Seq(
          YaspProcess("r1", Sql("my query"), None)
        ),
        sinks = Seq(
          YaspSink("r1", Dest.Parquet("path3", None))
        )
      )
    )
    assert(actual == expected)
  }
}
