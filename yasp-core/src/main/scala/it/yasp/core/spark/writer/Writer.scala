package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest.{Csv, Jdbc, Json, Parquet}
import org.apache.spark.sql.DataFrame

/** Writer
  *
  * Provide a write method to store a dataframe into a specific [[Dest]]
  * @tparam A:
  *   type param [[Dest]]
  */
trait Writer[A <: Dest] {

  /** Write a specific [[DataFrame]] to a specific [[Dest]] using spark primitives
    * @param dataFrame:
    *   a [[DataFrame]] instance
    * @param dest:
    *   A [[Dest]] instance
    */
  def write(dataFrame: DataFrame, dest: A): Unit
}

object Writer {

  private[writer] trait SparkWriteSupport {
    protected def writeDf(
        dataFrame: DataFrame,
        format: String,
        options: Map[String, String],
        partitionBy: Seq[String],
        mode: Option[String],
        path: Option[String]
    ): Unit = {
      val wr1 = dataFrame.write.format(format).options(options)
      val wr2 = mode.map(wr1.mode).getOrElse(wr1)
      val wr3 = if (partitionBy.isEmpty) wr2 else wr2.partitionBy(partitionBy: _*)
      path match {
        case Some(p) => wr3.save(p)
        case None    => wr3.save()
      }
    }
  }

  /** CsvWrite an implementation of Writer[Csv]
    */
  class CsvWriter extends Writer[Csv] with SparkWriteSupport {
    override def write(dataFrame: DataFrame, dest: Csv): Unit =
      writeDf(dataFrame, format = "csv", dest.options, dest.partitionBy, dest.mode, Some(dest.csv))
  }

  /** CsvWrite an implementation of Writer[Csv]
    */
  class JsonWriter extends Writer[Json] with SparkWriteSupport {
    override def write(dataFrame: DataFrame, dest: Json): Unit =
      writeDf(
        dataFrame,
        format = "json",
        dest.options,
        dest.partitionBy,
        dest.mode,
        Some(dest.json)
      )
  }

  class JdbcWriter extends Writer[Jdbc] with SparkWriteSupport {
    override def write(dataFrame: DataFrame, dest: Jdbc): Unit = {
      val opts = dest.options ++ Map(
        "url"      -> dest.jdbcUrl,
        "user"     -> dest.jdbcAuth.map(_.username).getOrElse(""),
        "password" -> dest.jdbcAuth.map(_.password).getOrElse("")
      )
      writeDf(dataFrame, format = "jdbc", opts, Seq.empty, dest.mode, None)
    }
  }

  /** ParquetWriter an implementation of Writer[Parquet]
    */
  class ParquetWriter extends Writer[Parquet] with SparkWriteSupport {
    override def write(dataFrame: DataFrame, dest: Parquet): Unit =
      writeDf(
        dataFrame,
        format = "parquet",
        Map.empty,
        dest.partitionBy,
        dest.mode,
        Some(dest.parquet)
      )
  }

  //TODO Something that retrieve automatically the relative Writer[A] should be implemented. Instead of doing it with an exhaustive pattern matching. probably shapeless could help on this
  /** DestWriter an implementation of Writer[Dest]
    *
    * Provide a method to dispatch the write request at the specific Writer implementation
    */
  class DestWriter extends Writer[Dest] {
    override def write(dataFrame: DataFrame, dest: Dest): Unit =
      dest match {
        case d: Parquet => new ParquetWriter().write(dataFrame, d)
        case d: Csv     => new CsvWriter().write(dataFrame, d)
        case d: Jdbc    => new JdbcWriter().write(dataFrame, d)
        case d: Json    => new JsonWriter().write(dataFrame, d)
      }
  }
}
