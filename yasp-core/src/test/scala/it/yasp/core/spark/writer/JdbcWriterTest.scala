package it.yasp.core.spark.writer

import it.yasp.core.spark.model.{BasicCredentials, Dest}
import it.yasp.core.spark.writer.Writer.JdbcWriter
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager.{getConnection, registerDriver}
import java.util.Properties

class JdbcWriterTest extends AnyFunSuite with SparkTestSuite {
  val writer = new JdbcWriter()

  val connUrl1: String = "jdbc:h2:mem:db3"
  val connUrl2: String = "jdbc:h2:mem:db4"

  registerDriver(new org.h2.Driver)
  val conn1: Connection = getConnection(connUrl1)
  val conn2: Connection = getConnection(connUrl2, "usr", "pwd")

  val df: Dataset[Row] = spark.createDataset(
    Seq(Row(1, "name1"), Row(2, "name2"), Row(3, "name3"), Row(4, "name4"))
  )(
    RowEncoder(
      StructType(
        Seq(
          StructField("ID", IntegerType, nullable = true),
          StructField("NAME", StringType, nullable = true)
        )
      )
    )
  )

  test("write database table") {
    writer.write(df, Dest.Jdbc(connUrl1, None, Map("dbTable" -> "my_test_table"), None))
    val actual = spark.read.jdbc(connUrl1, "my_test_table", new Properties())
    assertDatasetEquals(actual, df)
  }

  test("write database table with basic credentials") {
    writer.write(
      df,
      Dest.Jdbc(
        connUrl2,
        Some(BasicCredentials("usr", "pwd")),
        Map("dbTable" -> "my_test_table", "user" -> "usr", "password" -> "pwd"),
        None
      )
    )
    val properties = new Properties()
    properties.setProperty("user", "usr")
    properties.setProperty("password", "pwd")
    val actual     = spark.read.jdbc(connUrl2, "my_test_table", properties)
    assertDatasetEquals(actual, df)
  }

}
