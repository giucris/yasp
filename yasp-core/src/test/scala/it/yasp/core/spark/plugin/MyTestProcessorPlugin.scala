package it.yasp.core.spark.plugin
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class MyTestProcessorPlugin extends ProcessorPlugin {

  override def process(sparkSession: SparkSession, options: Option[Map[String, String]]): Dataset[Row] =
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
