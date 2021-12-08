package it.yasp.core.spark

import it.yasp.core.spark.reader.{CsvReaderTest, ParquetReaderTest}
import it.yasp.core.spark.session.SparkSessionFactoryTest
import org.scalatest.Stepwise

class YaspCoreSuites
    extends Stepwise(
      new SparkSessionFactoryTest,
      new CsvReaderTest,
      new ParquetReaderTest
    )
