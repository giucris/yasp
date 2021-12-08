package it.yasp.core.spark.reader

import it.yasp.core.spark.testutils.SparkTestSuite
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Assertion, BeforeAndAfterAll, DoNotDiscover}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.reflect.io.Path

@DoNotDiscover
class CsvReaderTest extends AnyFunSuite with SparkTestSuite {

  private val workspace = "yasp-core/src/test/resources/CsvReaderTest"

  override protected def beforeAll(): Unit = {
    cleanWorkspace(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    cleanWorkspace(workspace)
    super.afterAll()
  }

  test("read without header") {
    createFile(Seq("h1,h2,h3", "a,b,c"), s"$workspace/read1/file1.csv")
    val session  = SparkSession.builder().master("local[*]").getOrCreate()
    val expected = session.createDataset(Seq(Row("h1", "h2", "h3"), Row("a", "b", "c")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("_c0", StringType, nullable = true),
            StructField("_c1", StringType, nullable = true),
            StructField("_c2", StringType, nullable = true)
          )
        )
      )
    )
    val actual   = new CsvReader(session).read(s"$workspace/read1/file1.csv", header = false)
    assertDatasetEquals(actual, expected)
    session.stop()
  }

  test("read with header") {
    createFile(Seq("h1,h2,h3", "a,b,c"), s"$workspace/read1/file2.csv")
    val session  = SparkSession.builder().master("local[*]").getOrCreate()
    val expected = session.createDataset(Seq(Row("a", "b", "c")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("h1", StringType, nullable = true),
            StructField("h2", StringType, nullable = true),
            StructField("h3", StringType, nullable = true)
          )
        )
      )
    )
    val actual   = new CsvReader(session).read(s"$workspace/read1/file2.csv", header = true)
    assertDatasetEquals(actual, expected)
    session.stop()
  }

  def createFile(content: Seq[String], filePath: String): Unit = {
    Files.createDirectories(Paths.get(filePath).getParent)
    Files.write(Files.createFile(Paths.get(filePath)), content.asJava)
  }

  def cleanWorkspace(path: String): Unit =
    Path(path).deleteRecursively()
}
