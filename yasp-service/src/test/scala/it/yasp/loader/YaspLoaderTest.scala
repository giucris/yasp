package it.yasp.loader

import it.yasp.core.spark.model.Source
import it.yasp.core.spark.model.Source.Parquet
import it.yasp.core.spark.reader.Reader
import it.yasp.core.spark.registry.Registry
import it.yasp.loader.YaspLoader.DefaultYaspLoader
import it.yasp.model.YaspSource
import it.yasp.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspLoaderTest extends AnyFunSuite with SparkTestSuite with MockFactory {

  test("load register source") {
    val reader   = mock[Reader[Source]]
    val registry = mock[Registry]

    inSequence(
      (reader.read _)
        .expects(Parquet(Seq("x", "y"), mergeSchema = false))
        .once()
        .returns(
          spark.createDataset(Seq(Row("a")))(
            RowEncoder(StructType(Seq(StructField("h1", StringType, nullable = true))))
          )
        ),
      (registry.register _)
        .expects(*, "my_table")
        .once()
    )

    new DefaultYaspLoader(reader, registry).load(
      YaspSource("my_table", Parquet(Seq("x", "y"), mergeSchema = false))
    )
  }
}
