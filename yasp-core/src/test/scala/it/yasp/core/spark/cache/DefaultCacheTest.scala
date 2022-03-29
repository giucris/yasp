package it.yasp.core.spark.cache

import it.yasp.core.spark.cache.Cache.DefaultCache
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.testkit.{SparkTestSuite, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

@DoNotDiscover
class DefaultCacheTest extends AnyFunSuite with SparkTestSuite with BeforeAndAfterAll {
  private val workspace = "yasp-core/src/test/resources/DefaultCacheTest/"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }

  val cache = new DefaultCache()

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
    assert(cache.cache(ds1, Memory).storageLevel == StorageLevel.MEMORY_ONLY)
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
    assert(cache.cache(ds2, Disk).storageLevel == StorageLevel.DISK_ONLY)
  }

  test("cache with MemoryAndDisk") {
    val ds2 = spark.createDataset(Seq(Row("g", "h", "i")))(
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
    assert(cache.cache(ds2, MemoryAndDisk).storageLevel == StorageLevel.MEMORY_AND_DISK)
  }

  test("cache with MemorySer") {
    val ds2 = spark.createDataset(Seq(Row("l", "m", "n")))(
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
    assert(new DefaultCache().cache(ds2, MemorySer).storageLevel == StorageLevel.MEMORY_ONLY_SER)
  }

  test("cache with MemoryAndDiskSer") {
    val ds2 = spark.createDataset(Seq(Row("o", "p", "q")))(
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
    assert(
      new DefaultCache()
        .cache(ds2, MemoryAndDiskSer)
        .storageLevel == StorageLevel.MEMORY_AND_DISK_SER
    )
  }

}
