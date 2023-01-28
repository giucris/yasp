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
      s"""
        |session:
        |  kind: Local
        |  name: example-app
        |  conf: {}
        |plan:
        |  actions:
        |    - id: read_customer
        |      dataset: customer
        |      source:
        |        format: csv
        |        options:
        |          path: $workspace/test1/source/user.csv
        |          header: 'true'
        |          sep: ','
        |      cache: Memory
        |    - id: read_customer_addresses
        |      dataset: customer_addresses
        |      source:
        |        format: json
        |        options:
        |          path: $workspace/test1/source/addresses.jsonl
        |    - id: read_customer_with_address
        |      dataset: customer_with_address
        |      process:
        |        query: >-
        |          SELECT c.name,c.surname,a.address
        |          FROM customer c JOIN customer_addresses a ON c.id = a.user_id
        |    - id: sink_customer_with_address
        |      dataset: customer_with_address
        |      dest:
        |        format: parquet
        |        options:
        |          path: $workspace/test1/output/
        |""".stripMargin,
      dryRun = false
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
        s"""session:
          |  kind: Local
          |  name: example-app
          |  conf: {}
          |plan:
          |  actions:
          |    - id: read_user_csv
          |      dataset: users_table
          |      source:
          |        format: csv
          |        options:
          |          header: 'true'
          |          sep: ','
          |          path: $workspace/test2/source/user.csv
          |      cache: Memory
          |    - id: read_address_json
          |      dataset: addresses_table
          |      source:
          |        format: json
          |        options:
          |         path: $workspace/test2/source/addresses.jsonl
          |    - id: join_user_address
          |      dataset: user_with_address_table
          |      process:
          |        query: SELECT u.name,u.surname,a.address FROM users_table u JOIN addresses_table a ON u.id = a.user_id
          |    - id: sink_user_address
          |      dataset: user_with_address_table
          |      dest:
          |        format: parquet
          |        options:
          |          path: $workspace/test2/output/
          |""".stripMargin
      )
    )

    YaspApp.fromFile(s"$workspace/test2/execution/example.yml", dryRun = false)

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
