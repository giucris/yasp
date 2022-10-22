package it.yasp.core.spark.plugin
import org.apache.spark.sql.{Dataset, Row}

class MyTestWriterPlugin extends WriterPlugin {

  override def write(dataset: Dataset[Row], options: Option[Map[String, String]]): Unit = {
    options.flatMap(_.get("path")) match {
      case Some(value) => dataset.write.parquet(value)
      case None => ()
    }
  }
}
