package it.yasp.core.spark.reader

import it.yasp.core.spark.plugin.ReaderPlugin
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class MyTestReaderPluing extends ReaderPlugin {
  override def read(sparkSession: SparkSession, options: Option[Map[String, String]]): Dataset[Row] =
    sparkSession.createDataset(Seq(Row(1, "x"), Row(2, "y")))(
      RowEncoder(
        StructType(
          Seq(
            StructField("ID", IntegerType, nullable = true),
            StructField("FIELD1", StringType, nullable = true)
          )
        )
      )
    )
}
