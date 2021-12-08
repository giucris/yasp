package it.yasp.core.spark.reader

import it.yasp.core.spark.model.Csv
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** CsvReader
  * @param spark
  *   a [[SparkSession]] instance
  */
class CsvReader(spark: SparkSession) {

  /** Read csv files on the path provided and return as [[Dataset]] of [[Row]]
    * @param path
    *   the csv path as [[String]]
    * @param header
    *   a [[Boolean]] that describe if there is header on the csv files
    * @param separator
    *   a separator that will be used to parse the input csv files
    * @return
    */
  def read(csv: Csv): Dataset[Row] =
    spark.read.option("header", csv.header).option("sep", csv.separator).csv(csv.path)
}
