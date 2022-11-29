package it.yasp.core.spark.operators

import it.yasp.core.spark.err.YaspCoreError.RepartitionOperationError
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class DataOperatorsTest extends AnyFunSuite with SparkTestSuite {

  val operators = new DataOperators()

  test("cache with Memory") {
    val ds1    = spark.createDataset(Seq(Row("a", "b", "c")))(
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
    val actual = operators.cache(ds1, Memory).map(_.storageLevel)
    assert(actual == Right(StorageLevel.MEMORY_ONLY))
  }

  test("cache with Disk") {
    val ds2    = spark.createDataset(Seq(Row("d", "e", "f")))(
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
    val actual = operators.cache(ds2, Disk).map(_.storageLevel)
    assert(actual == Right(StorageLevel.DISK_ONLY))
  }

  test("cache with MemoryAndDisk") {
    val ds3    = spark.createDataset(Seq(Row("g", "h", "i")))(
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
    val actual = operators.cache(ds3, MemoryAndDisk).map(_.storageLevel)
    assert(actual == Right(StorageLevel.MEMORY_AND_DISK))
  }

  test("cache with MemorySer") {
    val ds4    = spark.createDataset(Seq(Row("l", "m", "n")))(
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
    val actual = operators.cache(ds4, MemorySer).map(_.storageLevel)
    assert(actual == Right(StorageLevel.MEMORY_ONLY_SER))
  }

  test("cache with MemoryAndDiskSer") {
    val ds5    = spark.createDataset(Seq(Row("o", "p", "q")))(
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
    val actual = operators.cache(ds5, MemoryAndDiskSer).map(_.storageLevel)
    assert(actual == Right(StorageLevel.MEMORY_AND_DISK_SER))
  }

  test("repartition") {
    val ds6    = spark.createDataset(Seq(Row("a"), Row("b"), Row("c"), Row("d"), Row("e"), Row("f")))(
      RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
    )
    val actual = operators.repartition(ds6, 2).map(_.rdd.getNumPartitions)
    assert(actual == Right(2))
  }

  test("repartition return RepartitionOperationError") {
    val ds6    = spark.createDataset(Seq(Row("a"), Row("b"), Row("c"), Row("d"), Row("e"), Row("f")))(
      RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
    )
    val actual = operators.repartition(ds6, 0)
    assert(actual.left.getOrElse(fail()).isInstanceOf[RepartitionOperationError])
  }
}
