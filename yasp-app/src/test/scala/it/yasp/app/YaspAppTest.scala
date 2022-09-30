package it.yasp.app

import it.yasp.testkit.SparkTestSuite
import it.yasp.testkit.TestUtils.{cleanFolder, createFile}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class YaspAppTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  private val workspace = "yasp-app/src/test/resources/YaspApp"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cleanFolder(workspace)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    cleanFolder(workspace)
  }

  test("fromYaml") {
    createFile(
      filePath = s"$workspace/test1/source/user.csv",
      rows = Seq(
        "id,name,surname,age",
        "1,tester,scala,18",
        "2,coder,spark,22"
      )
    )
    createFile(
      filePath = s"$workspace/test1/source/addresses.jsonl",
      rows = Seq(
        "{\"user_id\":\"1\",\"address\":\"street1\"}",
        "{\"user_id\":\"2\",\"address\":\"street2\"}"
      )
    )

    YaspApp.fromYaml(
      """session:
        |  kind: Local
        |  name: example-app
        |  conf: {}
        |plan:
        |  sources:
        |    - id: users
        |      source:
        |        format: csv
        |        options:
        |          path: yasp-app/src/test/resources/YaspApp/test1/source/user.csv
        |          header: 'true'
        |          sep: ','
        |      cache: Memory
        |    - id: addresses
        |      source:
        |        format: json
        |        options:
        |          path: yasp-app/src/test/resources/YaspApp/test1/source/addresses.jsonl
        |  processes:
        |    - id: user_with_address
        |      process:
        |        query: >-
        |          SELECT u.name,u.surname,a.address
        |          FROM users u JOIN addresses a ON u.id = a.user_id
        |  sinks:
        |    - id: user_with_address
        |      dest:
        |        format: parquet
        |        options:
        |          path: yasp-app/src/test/resources/YaspApp/test1/output/
        |""".stripMargin
    )

    val actual   = spark.read.parquet(s"$workspace/test1/output/")
    val expected = spark.createDataset(
      Seq(
        Row("coder", "spark", "street2"),
        Row("tester", "scala", "street1")
      )
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("name", StringType, nullable = true),
            StructField("surname", StringType, nullable = true),
            StructField("address", StringType, nullable = true)
          )
        )
      )
    )

    assertDatasetEquals(actual, expected)
  }

  test("fromFile") {
    createFile(
      filePath = s"$workspace/test2/source/user.csv",
      rows = Seq(
        "id,name,surname,age",
        "1,tester,scala,18",
        "2,coder,spark,22"
      )
    )
    createFile(
      filePath = s"$workspace/test2/source/addresses.jsonl",
      rows = Seq(
        "{\"user_id\":\"1\",\"address\":\"street1\"}",
        "{\"user_id\":\"2\",\"address\":\"street2\"}"
      )
    )
    createFile(
      filePath = s"$workspace/test2/execution/example.yml",
      rows = Seq(
        """session:
          |  kind: Local
          |  name: example-app
          |  conf: {}
          |plan:
          |  sources:
          |    - id: users_file
          |      source:
          |        format: csv
          |        options:
          |          header: 'true'
          |          sep: ','
          |          path: yasp-app/src/test/resources/YaspApp/test2/source/user.csv
          |      cache: Memory
          |    - id: addresses_file
          |      source:
          |        format: json
          |        options:
          |         path: yasp-app/src/test/resources/YaspApp/test2/source/addresses.jsonl
          |  processes:
          |    - id: user_with_address_file
          |      process:
          |        query: SELECT u.name,u.surname,a.address FROM users_file u JOIN addresses_file a ON u.id = a.user_id
          |  sinks:
          |    - id: user_with_address_file
          |      dest:
          |        format: parquet
          |        options:
          |          path: yasp-app/src/test/resources/YaspApp/test2/output/
          |""".stripMargin
      )
    )

    YaspApp.fromFile(s"$workspace/test2/execution/example.yml")

    val actual   = spark.read.parquet(s"$workspace/test2/output/")
    val expected = spark.createDataset(
      Seq(
        Row("coder", "spark", "street2"),
        Row("tester", "scala", "street1")
      )
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("name", StringType, nullable = true),
            StructField("surname", StringType, nullable = true),
            StructField("address", StringType, nullable = true)
          )
        )
      )
    )

    assertDatasetEquals(actual, expected)
  }
}
