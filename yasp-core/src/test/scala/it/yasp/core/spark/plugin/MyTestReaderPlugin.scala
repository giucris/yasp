package it.yasp.core.spark.plugin

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class MyTestReaderPlugin extends ReaderPlugin {
  override def read(sparkSession: SparkSession, options: Option[Map[String, String]]): Dataset[Row] =
    sparkSession.createDataset(Seq.empty[Row])(
      RowEncoder(StructType(Seq(StructField("ID", IntegerType, nullable = true))))
    )

}
