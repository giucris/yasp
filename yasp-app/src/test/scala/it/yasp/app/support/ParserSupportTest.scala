package it.yasp.app.support

import io.circe.generic.auto._
import it.yasp.app.err.YaspError.ParseYmlError
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.SessionType.{Distributed, Local}
import it.yasp.core.spark.model._
import it.yasp.service.model._
import org.scalatest.funsuite.AnyFunSuite

class ParserSupportTest extends AnyFunSuite with ParserSupport {

  test("parse return Right") {
    val expected = YaspExecution(
      Session(
        kind = Distributed,
        name = "my-app-name",
        conf = Some(Map("key-1" -> "value", "key-2" -> "value")),
        withHiveSupport = Some(true),
        withCheckpointDir = Some("xyz")
      ),
      YaspPlan(
        Seq(
          YaspSource(
            "id1",
            Source.Format("csv", options = Map("path" -> "x", "header" -> "false", "sep" -> ",")),
            cache = Some(Memory)
          ),
          YaspSource(
            "id2",
            Source.Format("parquet", options = Map("path" -> "x")),
            cache = Some(MemoryAndDisk)
          ),
          YaspSource(
            "id3",
            Source.Format(
              "jdbc",
              options = Map("url" -> "url", "user" -> "x", "password" -> "y", "dbTable" -> "table")
            ),
            cache = Some(Disk)
          ),
          YaspSource(
            "id4",
            Source.Format("csv", options = Map("path" -> "z")),
            cache = Some(Checkpoint)
          ),
          YaspSource(
            "id5",
            Source.Format("csv", options = Map("path" -> "k")),
            cache = Some(MemorySer)
          )
        ),
        Seq(
          YaspProcess("p1", Sql("my-query")),
          YaspProcess("p2", Sql("my-query-2"), cache = Some(MemoryAndDiskSer))
        ),
        Seq(
          YaspSink("p1", Dest.Format("parquet", Map("path" -> "out-path-1"))),
          YaspSink(
            "p3",
            Dest.Format("parquet", Map("path" -> "out-path-2"), partitionBy = Seq("col1", "col2"))
          )
        )
      )
    )

    val actual = parseYaml[YaspExecution](
      """
        |session:
        |  kind: Distributed
        |  name: my-app-name
        |  withCheckpointDir: xyz
        |  withHiveSupport: true
        |  conf:
        |    key-1: value
        |    key-2: value
        |plan:
        |  sources:
        |  - id: id1
        |    source:
        |      format: csv
        |      options:
        |        path: x
        |        header: 'false'
        |        sep: ','
        |    cache: Memory
        |  - id: id2
        |    source:
        |      format: parquet
        |      options:
        |        path: x
        |    cache: MemoryAndDisk
        |  - id: id3
        |    source:
        |      format: jdbc
        |      options:
        |        url: url
        |        user: x
        |        password: y
        |        dbTable: table
        |    cache: Disk
        |  - id: id4
        |    source:
        |      format: csv
        |      options:
        |        path: z
        |    cache: Checkpoint
        |  - id: id5
        |    source:
        |      format: csv
        |      options:
        |        path: k
        |    cache: MemorySer
        |  processes:
        |  - id: p1
        |    process:
        |      query: my-query
        |  - id: p2
        |    process:
        |      query: my-query-2
        |    cache: MemoryAndDiskSer
        |  sinks:
        |  - id: p1
        |    dest:
        |      format: parquet
        |      options:
        |        path: out-path-1
        |  - id: p3
        |    dest:
        |      format: parquet
        |      options:
        |        path: out-path-2
        |      partitionBy:
        |        - col1
        |        - col2
        |""".stripMargin
    )
    assert(actual == Right(expected))
  }

  test("parse return Right with lower Session Kind string") {
    val expected = YaspExecution(
      Session(
        kind = Local,
        name = "my-app-name"
      ),
      YaspPlan(
        Seq(
          YaspSource(
            "id1",
            Source.Format("csv", options = Map("path" -> "x", "header" -> "false", "sep" -> ",")),
            cache = Some(Memory)
          )
        ),
        Seq(
          YaspProcess("p1", Sql("my-query"))
        ),
        Seq(
          YaspSink("p1", Dest.Format("parquet", Map("path" -> "out-path-1")))
        )
      )
    )

    val actual = parseYaml[YaspExecution](
      """
        |session:
        |  kind: local
        |  name: my-app-name
        |plan:
        |  sources:
        |  - id: id1
        |    source:
        |      format: csv
        |      options:
        |        path: x
        |        header: 'false'
        |        sep: ','
        |    cache: Memory
        |  processes:
        |  - id: p1
        |    process:
        |      query: my-query
        |  sinks:
        |  - id: p1
        |    dest:
        |      format: parquet
        |      options:
        |        path: out-path-1
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
