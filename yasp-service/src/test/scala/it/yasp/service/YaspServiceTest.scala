package it.yasp.service

import it.yasp.core.spark.model.Dest.Format
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.SessionType.Local
import it.yasp.core.spark.model.{Session, Source}
import it.yasp.service.model._
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class YaspServiceTest extends AnyFunSuite with SparkTestSuite with MockFactory with BeforeAndAfterAll {

  private val workspace = "yasp-service/src/test/resources/YaspExecutorTest"

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

    YaspService().run(
      YaspExecution(
        session = Session(Local, "my-app-name", None, None, None, None),
        plan = YaspPlan(
          sources = Seq(
            YaspSource(
              id = "data_1",
              source = Source.Format(
                "csv",
                options = Map(
                  "header" -> "true",
                  "sep"    -> ",",
                  "path"   -> s"$workspace/csv-data-source-1/file1.csv"
                )
              )
            ),
            YaspSource(
              id = "data_2",
              source = Source.Format(
                "csv",
                options = Map(
                  "header" -> "true",
                  "sep"    -> ",",
                  "path"   -> s"$workspace/csv-data-source-2/file1.csv"
                )
              )
            )
          ),
          processes = Seq(
            YaspProcess(
              id = "data_3",
              process = Sql("SELECT d1.*,d2.city,d2.address FROM data_1 d1 JOIN data_2 d2 ON d1.id=d2.id")
            )
          ),
          sinks = Seq(
            YaspSink(
              id = "data_3",
              dest = Format("parquet", options = Map("path" -> s"$workspace/parquet-out/"))
            )
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

    assertDatasetEquals(actual, expected)
  }

}
