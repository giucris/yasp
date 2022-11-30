package it.yasp.core.spark.operators

import it.yasp.core.spark.err.YaspCoreError.RepartitionOperationError
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.model.DataOperations
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
    val actual = operators.exec(ds1, DataOperations(None, Some(Memory))).map(_.storageLevel)
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
    val actual = operators.exec(ds2, DataOperations(None, Some(Disk))).map(_.storageLevel)
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
    val actual = operators.exec(ds3, DataOperations(None, Some(MemoryAndDisk))).map(_.storageLevel)
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
    val actual = operators.exec(ds4, DataOperations(None, Some(MemorySer))).map(_.storageLevel)
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
    val actual = operators.exec(ds5, DataOperations(None, Some(MemoryAndDiskSer))).map(_.storageLevel)
    assert(actual == Right(StorageLevel.MEMORY_AND_DISK_SER))
  }

  test("repartition") {
    val ds6    = spark.createDataset(Seq(Row("a"), Row("b"), Row("c"), Row("d"), Row("e"), Row("f")))(
      RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
    )
    val actual = operators.exec(ds6, DataOperations(Some(2), None)).map(_.rdd.getNumPartitions)
    assert(actual == Right(2))
  }

  test("cache and repartition") {
    val ds7    = spark.createDataset(Seq(Row("a", "b", "c")))(
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
    val actual = operators.exec(ds7, DataOperations(Some(2), Some(Memory)))

    assert(actual.map(_.storageLevel) == Right(StorageLevel.MEMORY_ONLY))
    assert(actual.map(_.rdd.getNumPartitions) == Right(2))
  }

  test("repartition return RepartitionOperationError") {
    val ds8    = spark.createDataset(Seq(Row("a"), Row("b"), Row("c"), Row("d"), Row("e"), Row("f")))(
      RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
    )
    val actual = operators.exec(ds8, DataOperations(Some(0), None))
    assert(actual.left.getOrElse(fail()).isInstanceOf[RepartitionOperationError])
  }
}
