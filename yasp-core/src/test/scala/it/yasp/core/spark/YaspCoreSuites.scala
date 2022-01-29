package it.yasp.core.spark

import it.yasp.core.spark.reader.{
  CsvDataSourceReaderTest,
  JDBCDataSourceReaderTest,
  ParquetDataSourceReaderTest
}
import it.yasp.core.spark.session.SparkSessionFactoryTest
import org.scalatest.Stepwise

class YaspCoreSuites
    extends Stepwise(
      new SparkSessionFactoryTest,
      new CsvDataSourceReaderTest,
      new ParquetDataSourceReaderTest,
      new JDBCDataSourceReaderTest
    )
