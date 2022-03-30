package it.yasp.service.loader

import it.yasp.core.spark.cache.Cache
import it.yasp.core.spark.model.CacheLayer.Memory
import it.yasp.core.spark.model.Source
import it.yasp.core.spark.model.Source.Parquet
import it.yasp.core.spark.reader.Reader
import it.yasp.core.spark.registry.Registry
import it.yasp.service.loader.YaspLoader.DefaultYaspLoader
import it.yasp.service.model.YaspSource
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspLoaderTest extends AnyFunSuite with SparkTestSuite with MockFactory {

  val reader: Reader[Source] = mock[Reader[Source]]
  val cache: Cache           = mock[Cache]
  val registry: Registry     = mock[Registry]

  val yaspLoader: YaspLoader = new DefaultYaspLoader(reader, registry, cache)

  test("load will read and register source") {
    inSequence(
      (reader.read _)
        .expects(Parquet("x", mergeSchema = false))
        .once()
        .returns(
          spark.createDataset(Seq(Row("b")))(
            RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
          )
        ),
      (registry.register _)
        .expects(*, "tbl")
        .once()
    )

    yaspLoader.load(YaspSource("tbl", Parquet("x", mergeSchema = false), None))
  }

  test("load will read cache and register source") {
    inSequence(
      (reader.read _)
        .expects(Parquet("x", mergeSchema = false))
        .once()
        .returns(
          spark.createDataset(Seq(Row("a")))(
            RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
          )
        ),
      (cache.cache _)
        .expects(*, Memory)
        .once()
        .returns(
          spark.createDataset(Seq(Row("a")))(
            RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
          )
        ),
      (registry.register _)
        .expects(*, "tbl")
        .once()
    )

    yaspLoader.load(YaspSource("tbl", Parquet("x", mergeSchema = false), Some(Memory)))
  }

}
