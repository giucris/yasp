package it.yasp.core.spark.reader

import it.yasp.core.spark.model.BasicCredentials
import it.yasp.core.spark.model.DataSource.JDBC
import it.yasp.core.spark.reader.DataSourceReader.JDBCDataSourceReader
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.h2.Driver
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager._
import java.sql.{Connection, DriverManager}

@DoNotDiscover
class JDBCDataSourceReaderTest extends AnyFunSuite with SparkTestSuite {

  val connUrl1: String  = "jdbc:h2:mem:db1"
  val connUrl2: String  = "jdbc:h2:mem:db2"
  registerDriver(new Driver)
  val conn1: Connection = getConnection(connUrl1)
  val conn2: Connection = getConnection(connUrl2, "usr", "pwd")

  override protected def beforeAll(): Unit = {
    executeStatement(
      conn1,
      "CREATE TABLE table_1(id INT NOT NULL, name VARCHAR(20) NOT NULL,PRIMARY KEY(id)"
    )
    val DDL = "CREATE TABLE test_table (" +
      "id INT NOT NULL, " +
      "name VARCHAR(20) NOT NULL, " +
      "PRIMARY KEY (id))"
    val DML = "INSERT INTO test_table VALUES (1, 'test_data'), (2,'t2'), (3,'t3'),(4,'t4')"
    executeStatement(conn1, DDL)
    executeStatement(conn1, DML)
    executeStatement(conn2, DDL)
    executeStatement(conn2, DML)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    executeStatement(conn1, "DROP TABLE test_table")
    executeStatement(conn2, "DROP TABLE test_table")
    executeStatement(conn1, "SHUTDOWN")
    executeStatement(conn2, "SHUTDOWN")
  }

  test("read database table") {
    val expected = spark.createDataset(
      Seq(
        Row(1, "test_data"),
        Row(2, "t2"),
        Row(3, "t3"),
        Row(4, "t4")
      )
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
    val actual   = new JDBCDataSourceReader(spark).read(
      JDBC(url = connUrl1, table = "test_table", credentials = None)
    )
    assertDatasetEquals(actual, expected)
  }

  test("read database table with credentials") {
    val expected = spark.createDataset(
      Seq(
        Row(1, "test_data"),
        Row(2, "t2"),
        Row(3, "t3"),
        Row(4, "t4")
      )
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
    val actual   = new JDBCDataSourceReader(spark).read(
      JDBC(
        url = connUrl2,
        table = "test_table",
        credentials = Some(BasicCredentials("usr", "pwd"))
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

    val actual = new JDBCDataSourceReader(spark)
      .read(
        JDBC(
          url = connUrl2,
          table = "(select ID from test_table where id=1) test",
          credentials = Some(BasicCredentials("usr", "pwd"))
        )
      )
    assertDatasetEquals(actual, expected)
  }

  private def executeStatement(conn: Connection, DDL: String): Unit = {
    val stmnt1 = conn.createStatement
    stmnt1.execute(DDL)
    stmnt1.close()
  }

}
