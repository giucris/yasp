package it.yasp.app

//TODO Fix YaspAppTest
/*
class YaspAppTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  private val workspace = "yasp-app/src/test/resources/YaspApp"

  override protected def afterAll(): Unit =
    cleanFolder(workspace)

  override protected def beforeAll(): Unit =
    cleanFolder(workspace)

  test("n source n transformation 1 write") {
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
    createFile(
      filePath = s"$workspace/test1/execution/example.yml",
      rows = Seq(
        """conf:
          |  sessionType:
          |    Local: {}
          |  appName: example-app
          |  config: {}
          |plan:
          |  sources:
          |    - id: users
          |      source:
          |        Csv:
          |          paths:
          |            - yasp-app/src/test/resources/YaspApp/test1/source/user.csv
          |          header: true
          |          separator: ','
          |    - id: addresses
          |      source:
          |        Json:
          |          paths:
          |            - yasp-app/src/test/resources/YaspApp/test1/source/addresses.jsonl
          |  processes:
          |    - id: user_with_address
          |      process:
          |        Sql:
          |          query: SELECT u.name,u.surname,a.address FROM users u JOIN addresses a ON u.id = a.user_id
          |  sinks:
          |    - id: user_with_address
          |      dest:
          |        Parquet:
          |          path: yasp-app/src/test/resources/YaspApp/test1/output/
          |""".stripMargin
      )
    )

    YaspApp.main(Array("-f", s"$workspace/test1/execution/example.yml"))

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

}
 */
