package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Avro
import it.yasp.core.spark.reader.Reader.AvroReader
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class AvroReaderTest extends AnyFunSuite with SparkTestSuite {

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
    expected.write.format("avro").save(s"$workspace/avro/fileWithoutSchema/")
    val actual   = new AvroReader(spark).read(Avro(Seq(s"$workspace/avro/fileWithoutSchema/")))

    assertDatasetEquals(actual, expected)
  }

}
