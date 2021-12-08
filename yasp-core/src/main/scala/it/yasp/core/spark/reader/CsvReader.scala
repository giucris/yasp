package it.yasp.core.spark.reader

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
    * @return
    */
  def read(path: String, header: Boolean): Dataset[Row] =
    spark.read.option("header", header).csv(path)
}