package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Source.Custom
import it.yasp.core.spark.plugin.PluginProvider
import it.yasp.core.spark.reader.Reader.CustomReader
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.{IntegerType, StringType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CustomReaderTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  val expectedDf: Dataset[Row] = spark.createDataset(Seq(Row(1, "x"), Row(2, "y")))(
    RowEncoder(
      StructType(
        Seq(
          StructField("ID", IntegerType, nullable = true),
          StructField("FIELD1", StringType, nullable = true)
        )
      )
    )
  )

  test("read load MyTestCustomReader") {
    val reader = new CustomReader(spark, new PluginProvider)
    val actual = reader.read(Custom("it.yasp.core.spark.plugin.MyTestReaderPlugin", Some(Map("x" -> "y"))))
    assertDatasetEquals(actual.getOrElse(fail()), expectedDf)
  }

}
