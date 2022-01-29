package it.yasp.testkit

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.funsuite.AnyFunSuite

class SparkTestSuiteTest extends AnyFunSuite with SparkTestSuite {

  test("assertDatasetEquals red with different schema on Dataset[Row]") {
    val ds1 = spark.createDataset(Seq(Row("data2", 1, 2), Row("data1", 1, null)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", DataTypes.StringType, nullable = true),
            StructField("a", DataTypes.IntegerType, nullable = true),
            StructField("b", DataTypes.IntegerType, nullable = true)
          )
        )
      )
    )
    val ds2 = spark.createDataset(Seq(Row("data2", 1, 2), Row("data1", 1, null)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", DataTypes.StringType, nullable = true),
            StructField("a", DataTypes.IntegerType, nullable = true),
            StructField("c", DataTypes.IntegerType, nullable = true)
          )
        )
      )
    )

    val caught          = intercept[TestFailedException] {
      assertDatasetEquals(ds1, ds2)
    }
    val expectedMessage =
      "StructType(StructField(d,StringType,true), StructField(a,IntegerType,true), StructField(b,IntegerType,true)) did not equal StructType(StructField(d,StringType,true), StructField(a,IntegerType,true), StructField(c,IntegerType,true))"
    assert(caught.getMessage == expectedMessage)
  }

  test("assertDatasetEquals red with different data on Dataset[Row]") {
    val ds1 = spark.createDataset(Seq(Row("data234", 1, 2, 3), Row("data1", 1, null, 2)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", DataTypes.StringType, nullable = true),
            StructField("a", DataTypes.IntegerType, nullable = true),
            StructField("b", DataTypes.IntegerType, nullable = true),
            StructField("c", DataTypes.IntegerType, nullable = true)
          )
        )
      )
    )
    val ds2 = spark.createDataset(Seq(Row("data2", 1, 2, 3), Row("data1", 1, null, 2)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", DataTypes.StringType, nullable = true),
            StructField("a", DataTypes.IntegerType, nullable = true),
            StructField("b", DataTypes.IntegerType, nullable = true),
            StructField("c", DataTypes.IntegerType, nullable = true)
          )
        )
      )
    )

    val caught = intercept[TestFailedException] {
      assertDatasetEquals(ds1, ds2)
    }

    val expectedStartsMessage =
      "Array([data1,1,null,2], [data234,1,2,3]) did not equal Array([data1,1,null,2], [data2,1,2,3])"
    assert(caught.getMessage == expectedStartsMessage)
  }

  test("assertDatasetEquals green with same data and schema on Dataset[Row]") {
    val ds1 = spark.createDataset(Seq(Row("data2", 1, 2, 3), Row("data1", 1, null, 2)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", DataTypes.StringType, nullable = true),
            StructField("a", DataTypes.IntegerType, nullable = true),
            StructField("b", DataTypes.IntegerType, nullable = true),
            StructField("c", DataTypes.IntegerType, nullable = true)
          )
        )
      )
    )
    val ds2 = spark.createDataset(Seq(Row("data2", 1, 2, 3), Row("data1", 1, null, 2)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", DataTypes.StringType, nullable = true),
            StructField("a", DataTypes.IntegerType, nullable = true),
            StructField("b", DataTypes.IntegerType, nullable = true),
            StructField("c", DataTypes.IntegerType, nullable = true)
          )
        )
      )
    )
    assertDatasetEquals(ds1, ds2)
  }

  test("assertDatasetEquals green with unsorted same data and schema Dataset[Row]") {
    val ds1 = spark.createDataset(Seq(Row("data1", 1, null, 2), Row("data2", 1, 2, 3)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", DataTypes.StringType, nullable = true),
            StructField("a", DataTypes.IntegerType, nullable = true),
            StructField("b", DataTypes.IntegerType, nullable = true),
            StructField("c", DataTypes.IntegerType, nullable = true)
          )
        )
      )
    )
    val ds2 = spark.createDataset(Seq(Row("data2", 1, 2, 3), Row("data1", 1, null, 2)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("d", DataTypes.StringType, nullable = true),
            StructField("a", DataTypes.IntegerType, nullable = true),
            StructField("b", DataTypes.IntegerType, nullable = true),
            StructField("c", DataTypes.IntegerType, nullable = true)
          )
        )
      )
    )
    assertDatasetEquals(ds1, ds2)
  }

}
