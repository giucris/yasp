package it.yasp.app.conf

import io.circe.generic.auto._
import it.yasp.core.spark.model.CacheLayer.{Memory, MemoryAndDisk}
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.{BasicCredentials, Dest, Source}
import it.yasp.core.spark.session.SessionConf
import it.yasp.core.spark.session.SessionType.Local
import it.yasp.service.model._
import org.scalatest.funsuite.AnyFunSuite

class ParserSupportTest extends AnyFunSuite with ParserSupport {

  test("parse") {
    val expected = YaspExecution(
      SessionConf(Local, "my-app-name", Map("key-1" -> "value", "key-2" -> "value")),
      YaspPlan(
        Seq(
          YaspSource("id1", Source.Csv("x", Some(Map("header" -> "false", "sep" -> ","))), Some(Memory)),
          YaspSource(
            "id2",
            Source.Parquet("x", mergeSchema = false),
            Some(MemoryAndDisk)
          ),
          YaspSource("id3", Source.Jdbc("url", "table", Some(BasicCredentials("x", "y"))), None),
          YaspSource("id4", Source.Csv("z", None), Some(Memory))
        ),
        Seq(
          YaspProcess("p1", Sql("my-query"), None),
          YaspProcess("p2", Sql("my-query"), None)
        ),
        Seq(
          YaspSink("p1", Dest.Parquet("out-path-1")),
          YaspSink("p3", Dest.Parquet("out-path-2"))
        )
      )
    )
    val actual   = parseYaml[YaspExecution](
      """
        |conf:
        |  sessionType: Local
        |  appName: my-app-name
        |  config:
        |    key-1: value
        |    key-2: value
        |plan:
        |  sources:
        |  - id: id1
        |    source:
        |      Csv:
        |        path: x
        |        options:
        |          header: 'false'
        |          sep: ','
        |    cache: Memory
        |  - id: id2
        |    source:
        |      Parquet:
        |        path: x
        |        mergeSchema: false
        |    cache: MemoryAndDisk
        |  - id: id3
        |    source:
        |      Jdbc:
        |        url: url
        |        table: table
        |        credentials:
        |          username: x
        |          password: y
        |  - id: id4
        |    source:
        |      Csv:
        |        path: z
        |    cache: Memory
        |  processes:
        |  - id: p1
        |    process:
        |      Sql:
        |        query: my-query
        |  - id: p2
        |    process:
        |      Sql:
        |        query: my-query
        |  sinks:
        |  - id: p1
        |    dest:
        |      Parquet:
        |        path: out-path-1
        |  - id: p3
        |    dest:
        |      Parquet:
        |        path: out-path-2
        |""".stripMargin
    )
    assert(actual == expected)
  }

}
