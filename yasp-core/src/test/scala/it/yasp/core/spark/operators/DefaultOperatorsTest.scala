package it.yasp.core.spark.operators

import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.operators.Operators.DefaultOperators
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

@DoNotDiscover
class DefaultOperatorsTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {

  override protected def beforeAll(): Unit =
    super.beforeAll()

  override protected def afterAll(): Unit =
    super.afterAll()

  val operators = new DefaultOperators()

  test("cache with Memory") {
    val ds1 = spark.createDataset(Seq(Row("a", "b", "c")))(
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
    assert(operators.cache(ds1, Memory).storageLevel == StorageLevel.MEMORY_ONLY)
  }

  test("cache with Disk") {
    val ds2 = spark.createDataset(Seq(Row("d", "e", "f")))(
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
    assert(operators.cache(ds2, Disk).storageLevel == StorageLevel.DISK_ONLY)
  }

  test("cache with MemoryAndDisk") {
    val ds3 = spark.createDataset(Seq(Row("g", "h", "i")))(
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
    assert(operators.cache(ds3, MemoryAndDisk).storageLevel == StorageLevel.MEMORY_AND_DISK)
  }

  test("cache with MemorySer") {
    val ds4 = spark.createDataset(Seq(Row("l", "m", "n")))(
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
    assert(operators.cache(ds4, MemorySer).storageLevel == StorageLevel.MEMORY_ONLY_SER)
  }

  test("cache with MemoryAndDiskSer") {
    val ds5 = spark.createDataset(Seq(Row("o", "p", "q")))(
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
    assert(operators.cache(ds5, MemoryAndDiskSer).storageLevel == StorageLevel.MEMORY_AND_DISK_SER)
  }

  test("repartition") {
    val ds6    = spark.createDataset(Seq(Row("a"), Row("b"), Row("c"), Row("d"), Row("e"), Row("f")))(
      RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
    )
    val actual = operators.repartition(ds6, 2)
    assert(actual.rdd.getNumPartitions == 2)
  }
}
