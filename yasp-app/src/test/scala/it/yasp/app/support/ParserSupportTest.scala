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
        sources = Seq(
          YaspSource(
            id = "id1",
            dataset = "data_1",
            Source.Format("csv", options = Map("path" -> "x", "header" -> "false", "sep" -> ",")),
            cache = Some(Memory)
          ),
          YaspSource(
            id = "id2",
            dataset = "data_2",
            Source.Format("parquet", options = Map("path" -> "x")),
            cache = Some(MemoryAndDisk)
          ),
          YaspSource(
            id = "id3",
            dataset = "data_3",
            Source.Format(
              "jdbc",
              options = Map("url" -> "url", "user" -> "x", "password" -> "y", "dbTable" -> "table")
            ),
            cache = Some(Disk)
          ),
          YaspSource(
            id = "id4",
            dataset = "data_4",
            Source.Format("csv", options = Map("path" -> "z")),
            cache = Some(Checkpoint)
          ),
          YaspSource(
            id = "id5",
            dataset = "data_5",
            Source.Format("csv", options = Map("path" -> "k")),
            cache = Some(MemorySer)
          ),
          YaspSource(
            id = "id6",
            dataset = "data_6",
            Source.Custom("x.y.z.CustomSource", options = Some(Map("path" -> "k"))),
            cache = Some(MemorySer)
          ),
          YaspSource(
            id = "id7",
            dataset = "data_7",
            Source.HiveTable("tbl"),
            cache = Some(MemorySer)
          )
        ),
        processes = Seq(
          YaspProcess(
            id = "p1",
            dataset = "data_8",
            Process.Custom("x.y.z.CustomProcess", options = Some(Map("x" -> "y")))
          ),
          YaspProcess(
            id = "p2",
            dataset = "data_9",
            Process.Sql("my-query")
          ),
          YaspProcess(
            id = "p3",
            dataset = "data_10",
            Process.Sql("my-query-2"),
            cache = Some(MemoryAndDiskSer)
          )
        ),
        sinks = Seq(
          YaspSink(
            id = "p1",
            dataset = "data_11",
            Dest.Format("parquet", Map("path" -> "out-path-1"), partitionBy = Seq("col1", "col2"))
          ),
          YaspSink(
            id = "p1",
            dataset = "data_12",
            Dest.Custom("x.y.z.CustomDest", Some(Map("path" -> "out-path-2")))
          ),
          YaspSink(
            id = "p3",
            dataset = "data_13",
            Dest.HiveTable("tbl")
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
        |    dataset: data_1
        |    source:
        |      format: csv
        |      options:
        |        path: x
        |        header: 'false'
        |        sep: ','
        |    cache: memory
        |  - id: id2
        |    dataset: data_2
        |    source:
        |      format: parquet
        |      options:
        |        path: x
        |    cache: Memory_And_Disk
        |  - id: id3
        |    dataset: data_3
        |    source:
        |      format: jdbc
        |      options:
        |        url: url
        |        user: x
        |        password: y
        |        dbTable: table
        |    cache: Disk
        |  - id: id4
        |    dataset: data_4
        |    source:
        |      format: csv
        |      options:
        |        path: z
        |    cache: Checkpoint
        |  - id: id5
        |    dataset: data_5
        |    source:
        |      format: csv
        |      options:
        |        path: k
        |    cache: MemorySer
        |  - id: id6
        |    dataset: data_6
        |    source:
        |      clazz: x.y.z.CustomSource
        |      options:
        |        path: k
        |    cache: MemorySer
        |  - id: id7
        |    dataset: data_7
        |    source:
        |      table: tbl
        |    cache: MemorySer
        |  processes:
        |  - id: p1
        |    dataset: data_8
        |    process:
        |      clazz: x.y.z.CustomProcess
        |      options:
        |        x: y
        |  - id: p2
        |    dataset: data_9
        |    process:
        |      query: my-query
        |  - id: p3
        |    dataset: data_10
        |    process:
        |      query: my-query-2
        |    cache: MemoryAndDiskSer
        |  sinks:
        |  - id: p1
        |    dataset: data_11
        |    dest:
        |      format: parquet
        |      options:
        |        path: out-path-1
        |      partitionBy:
        |        - col1
        |        - col2
        |  - id: p1
        |    dataset: data_12
        |    dest:
        |      clazz: x.y.z.CustomDest
        |      options:
        |        path: out-path-2
        |  - id: p3
        |    dataset: data_13
        |    dest:
        |      table: tbl
        |""".stripMargin
    )
    assert(actual == Right(expected))
  }

  test("parse return Right with lower session Kind and cache string") {
    val expected = YaspExecution(
      Session(
        kind = Local,
        name = "my-app-name"
      ),
      YaspPlan(
        Seq(
          YaspSource(
            id = "id1",
            dataset = "data_1",
            Source.Format("csv", options = Map("path" -> "x", "header" -> "false", "sep" -> ",")),
            cache = Some(Memory)
          )
        ),
        Seq(
          YaspProcess(
            id = "p1",
            dataset = "data_2",
            Sql("my-query")
          )
        ),
        Seq(
          YaspSink(
            id = "p1",
            dataset = "data_2",
            Dest.Format("parquet", Map("path" -> "out-path-1"))
          )
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
        |    dataset: data_1
        |    source:
        |      format: csv
        |      options:
        |        path: x
        |        header: 'false'
        |        sep: ','
        |    cache: MEMORY
        |  processes:
        |  - id: p1
        |    dataset: data_2
        |    process:
        |      query: my-query
        |  sinks:
        |  - id: p1
        |    dataset: data_2
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
