package it.yasp.executor

import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.{Dest, Source}
import it.yasp.core.spark.session.SessionType.Local
import it.yasp.core.spark.session.{SessionConf, SparkSessionFactory}
import it.yasp.executor.YaspExecutor.DefaultYaspExecutor
import it.yasp.model._
import it.yasp.service.YaspServiceFactory
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class YaspExecutorTest
    extends AnyFunSuite
    with MockFactory
    with BeforeAndAfterAll
    with SparkTestSuite {

  private val workspace = "yasp-app/src/test/resources/YaspExecutorTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("exec") {
    TestUtils.createFile(
      s"$workspace/csv-data-source-1/file1.csv",
      Seq(
        "id,name,surname",
        "1,name-1,surname-1",
        "2,name-2,surname-2"
      )
    )

    TestUtils.createFile(
      s"$workspace/csv-data-source-2/file1.csv",
      Seq(
        "id,city,address",
        "1,city-1,address-1",
        "2,city-2,address-2"
      )
    )

    val sparkSessionFactory = mock[SparkSessionFactory]

    (sparkSessionFactory.create _)
      .expects(SessionConf(Local, "my-app-name", Map.empty))
      .once()
      .returns(spark)

    new DefaultYaspExecutor(sparkSessionFactory, new YaspServiceFactory())
      .exec(
        YaspExecution(
          sessionConf = SessionConf(Local, "my-app-name", Map.empty),
          yaspPlan = YaspPlan(
            sources = Seq(
              YaspSource(
                "data_1",
                Source.Csv(
                  paths = Seq(s"$workspace/csv-data-source-1/file1.csv"),
                  header = true,
                  separator = ","
                )
              ),
              YaspSource(
                "data_2",
                Source.Csv(
                  paths = Seq(s"$workspace/csv-data-source-2/file1.csv"),
                  header = true,
                  separator = ","
                )
              )
            ),
            processes = Seq(
              YaspProcess(
                "data_3",
                Sql("SELECT d1.*,d2.city,d2.address FROM data_1 d1 JOIN data_2 d2 ON d1.id=d2.id")
              )
            ),
            sinks = Seq(
              YaspSink("data_3", Dest.Parquet(s"$workspace/parquet-out/"))
            )
          )
        )
      )

    val actual   = spark.read.parquet(s"$workspace/parquet-out/")
    val expected = spark.createDataset(
      Seq(
        Row("1", "name-1", "surname-1", "city-1", "address-1"),
        Row("2", "name-2", "surname-2", "city-2", "address-2")
      )
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("id", StringType, nullable = true),
            StructField("name", StringType, nullable = true),
            StructField("surname", StringType, nullable = true),
            StructField("city", StringType, nullable = true),
            StructField("address", StringType, nullable = true)
          )
        )
      )
    )

    assertDatasetEquals(actual,expected)
  }

}
