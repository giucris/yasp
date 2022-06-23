package it.yasp.core.spark.reader

import it.yasp.core.spark.model.BasicCredentials
import it.yasp.core.spark.model.Source.Jdbc
import it.yasp.core.spark.reader.Reader.JdbcReader
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Connection
import java.sql.DriverManager._

class JdbcReaderTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  val reader = new JdbcReader(spark)

  val connUrl1: String = "jdbc:h2:mem:db1"
  val connUrl2: String = "jdbc:h2:mem:db2"

  registerDriver(new org.h2.Driver)
  val conn1: Connection = getConnection(connUrl1)
  val conn2: Connection = getConnection(connUrl2, "usr", "pwd")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    executeStatement(
      conn = conn1,
      stmt = "CREATE TABLE my_table (id INT,name VARCHAR(20),PRIMARY KEY (id))"
    )
    executeStatement(
      conn = conn2,
      stmt = "CREATE TABLE my_table (id INT,name VARCHAR(20),PRIMARY KEY (id))"
    )
    executeStatement(
      conn = conn1,
      stmt = "INSERT INTO my_table VALUES (1, 'name1'), (2,'name2'), (3,'name3'),(4,'name4')"
    )
    executeStatement(
      conn = conn2,
      stmt = "INSERT INTO my_table VALUES (1, 'name1'), (2,'name2'), (3,'name3'),(4,'name4')"
    )
  }

  test("read database table") {
    val expected = spark.createDataset(
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
    val actual   = reader.read(Jdbc(connUrl1, options = Map("dbTable" -> "my_table")))
    assertDatasetEquals(actual, expected)
  }

  test("read database table with BasicCredentials") {
    val expected = spark.createDataset(
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
    val actual   = reader.read(
      Jdbc(
        jdbcUrl = connUrl2,
        jdbcAuth = Some(BasicCredentials("usr", "pwd")),
        options = Map("dbTable" -> "my_table")
      )
    )
    assertDatasetEquals(actual, expected)
  }

  test("read database query with credentials") {
    val expected = spark.createDataset(Seq(Row(1)))(
      RowEncoder(
        StructType(
          Seq(
            StructField("ID", IntegerType, nullable = true)
          )
        )
      )
    )

    val actual = reader.read(
      Jdbc(
        jdbcUrl = "jdbc:h2:mem:db2",
        jdbcAuth = Some(BasicCredentials("usr", "pwd")),
        options = Map("dbTable" -> "(select ID from my_table where id=1) test")
      )
    )
    assertDatasetEquals(actual, expected)
  }

  private def executeStatement(conn: Connection, stmt: String): Unit = {
    val statement = conn.createStatement
    statement.execute(stmt)
    statement.close()
  }

}
