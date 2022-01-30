package it.yasp.core.spark.reader

import it.yasp.core.spark.model.DataSource.Avro
import it.yasp.core.spark.reader.DataSourceReader.AvroDataSourceReader
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class AvroDataSourceReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/AvroReaderTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  test("DataReader read avro") {
    val expected = spark.createDataset(
      Seq(
        Row("data2", 1, 2, 3),
        Row("data1", 1, null, 2)
      )
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", StringType, nullable = true),
            StructField("a", IntegerType, nullable = true),
            StructField("b", IntegerType, nullable = true),
            StructField("c", IntegerType, nullable = true)
          )
        )
      )
    )
    expected.write.format("avro").save(s"$workspace/avro/fileWithoutSchema.avro")
    val actual   =
      new AvroDataSourceReader(spark).read(Avro(Seq(s"$workspace/avro/fileWithoutSchema.avro")))

    assertDatasetEquals(actual, expected)
  }

  test("DataReader read avro with schema") {
    spark
      .createDataset(Seq(Row("data2", "1", "2")))(
        RowEncoder(
          StructType(
            Seq(
              StructField("d", StringType, nullable = true),
              StructField("a", StringType, nullable = true),
              StructField("b", StringType, nullable = true)
            )
          )
        )
      )
      .write
      .mode(SaveMode.Append)
      .format("avro")
      .save(s"$workspace/avro/file1/")

    spark
      .createDataset(Seq(Row("data1", "1", null, "2")))(
        RowEncoder(
          StructType(
            Seq(
              StructField("d", StringType, nullable = true),
              StructField("a", StringType, nullable = true),
              StructField("b", StringType, nullable = true),
              StructField("c", StringType, nullable = true)
            )
          )
        )
      )
      .write
      .mode(SaveMode.Append)
      .format("avro")
      .save(s"$workspace/avro/file1/")

    val expected = spark.createDataset(
      Seq(
        Row("data2", "1", "2", null),
        Row("data1", "1", null, "2")
      )
    )(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", StringType, nullable = true),
            StructField("a", StringType, nullable = true),
            StructField("b", StringType, nullable = true),
            StructField("c", StringType, nullable = true)
          )
        )
      )
    )
    val actual   = new AvroDataSourceReader(spark).read(Avro(Seq(s"$workspace/avro/file1/")))

    assertDatasetEquals(actual, expected)
  }

}
