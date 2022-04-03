package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest._
import it.yasp.core.spark.model.{Dest, OutFormat}
import it.yasp.core.spark.writer.Writable.ops.WritableOps

trait Writable[A] {
  def format(a: A): OutFormat
}

object Writable {

  def apply[A](implicit ev: Writable[A]): Writable[A] = ev

  object ops {
    def format[A: Writable](a: A) = Writable[A].format(a)

    implicit class WritableOps[A: Writable](a: A) {
      def format: OutFormat = Writable[A].format(a)
    }
  }

  implicit def csvWritable: Writable[Csv] = new Writable[Csv] {
    override def format(a: Csv): OutFormat =
      OutFormat("csv", a.options ++ Map("path" -> a.path), a.partitionBy)
  }

  implicit def jsonWritable: Writable[Json] = new Writable[Json] {
    override def format(a: Json): OutFormat =
      OutFormat("json", a.options ++ Map("path" -> a.path), a.partitionBy)
  }

  implicit def parquetWritable: Writable[Parquet] = new Writable[Parquet] {
    override def format(a: Parquet): OutFormat =
      OutFormat("parquet", a.options ++ Map("path" -> a.path), a.partitionBy)
  }

  implicit def avroWritable: Writable[Avro] = new Writable[Avro] {
    override def format(a: Avro): OutFormat =
      OutFormat("avro", a.options ++ Map("path" -> a.path), a.partitionBy)
  }

  implicit def orcWritable: Writable[Orc] = new Writable[Orc] {
    override def format(a: Orc): OutFormat =
      OutFormat("orc", a.options ++ Map("path" -> a.path), a.partitionBy)
  }

  implicit def xmlWritable: Writable[Xml] = new Writable[Xml] {
    override def format(a: Xml): OutFormat =
      OutFormat("xml", a.options ++ Map("path" -> a.path), a.partitionBy)
  }

  implicit def jdbcWritable: Writable[Jdbc] = new Writable[Jdbc] {
    override def format(a: Jdbc): OutFormat = {
      val opts = Seq(
        Some("url"                   -> a.url),
        Some("table"                 -> a.table),
        a.credentials.map("user"     -> _.username),
        a.credentials.map("password" -> _.password)
      ).flatten.toMap
      OutFormat("jdbc", opts, Seq.empty)
    }
  }

  implicit def destWritable: Writable[Dest] = new Writable[Dest] {
    override def format(a: Dest): OutFormat =
      a match {
        case a: Csv     => a.format
        case a: Json    => a.format
        case a: Parquet => a.format
        case a: Orc     => a.format
        case a: Avro    => a.format
        case a: Xml     => a.format
        case a: Jdbc    => a.format
      }
  }

  implicit def outFormatWritable: Writable[OutFormat] = new Writable[OutFormat] {
    override def format(a: OutFormat): OutFormat = a
  }

}
