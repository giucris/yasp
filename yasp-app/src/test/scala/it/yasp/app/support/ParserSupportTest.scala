package it.yasp.app.support

import io.circe.generic.auto._
import it.yasp.app.err.YaspAppErrors.ParseYmlError
import it.yasp.core.spark.model.CacheLayer.{Checkpoint, Memory, MemoryAndDisk}
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.SessionType.Distributed
import it.yasp.core.spark.model._
import it.yasp.service.model._
import org.scalatest.funsuite.AnyFunSuite

class ParserSupportTest extends AnyFunSuite with ParserSupport {

  test("parse return Right") {
    val expected = YaspExecution(
      Session(Distributed, "my-app-name", Map("key-1" -> "value", "key-2" -> "value")),
      YaspPlan(
        Seq(
          YaspSource(
            "id1",
            Source.Csv("x", None,Map("header" -> "false", "sep" -> ",")),
            cache = Some(Memory)
          ),
          YaspSource(
            "id2",
            Source.Parquet("x"),
            cache = Some(MemoryAndDisk)
          ),
          YaspSource(
            "id3",
            Source.Jdbc("url", Some(BasicCredentials("x", "y")), Map("dbTable" -> "table"))
          ),
          YaspSource("id4", Source.Csv("z"), cache = Some(Checkpoint))
        ),
        Seq(
          YaspProcess("p1", Sql("my-query")),
          YaspProcess("p2", Sql("my-query-2"))
        ),
        Seq(
          YaspSink("p1", Dest.Parquet("out-path-1")),
          YaspSink("p3", Dest.Parquet("out-path-2", Seq("col1", "col2")))
        )
      )
    )
    val actual   = parseYaml[YaspExecution](
      """
        |session:
        |  kind: Distributed
        |  name: my-app-name
        |  conf:
        |    key-1: value
        |    key-2: value
        |plan:
        |  sources:
        |  - id: id1
        |    source:
        |      csv: x
        |      options:
        |        header: 'false'
        |        sep: ','
        |    cache: Memory
        |  - id: id2
        |    source:
        |      parquet: x
        |    cache: MemoryAndDisk
        |  - id: id3
        |    source:
        |      jdbcUrl: url
        |      jdbcAuth:
        |        username: x
        |        password: y
        |      options:
        |        dbTable: table
        |  - id: id4
        |    source:
        |      csv: z
        |    cache: Checkpoint
        |  processes:
        |  - id: p1
        |    process:
        |      query: my-query
        |  - id: p2
        |    process:
        |      query: my-query-2
        |  sinks:
        |  - id: p1
        |    dest:
        |      parquet: out-path-1
        |  - id: p3
        |    dest:
        |      parquet: out-path-2
        |      partitionBy:
        |        - col1
        |        - col2
        |""".stripMargin
    )
    assert(actual == Right(expected))
  }

  test("parse return Left") {
    val actual = parseYaml[YaspExecution](
      """
        |session:
        |plan:
        |  sources:
        |  - id: id3
        |  - id: p3
        |    dest:
        |      Parquet:
        |        path: out-path-2
        |        partitionBy:
        |          - col1
        |          - col2
        |""".stripMargin
    )
    assert(actual.isLeft)
    assert(actual.left.get.isInstanceOf[ParseYmlError])
  }

}
