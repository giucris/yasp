package it.yasp.core.spark

import it.yasp.core.spark.reader.CsvReaderTest
import it.yasp.core.spark.session.SparkSessionFactoryTest
import org.scalatest.{Stepwise, Suite}

class YaspCoreSparkSuites
    extends Stepwise(
      new SparkSessionFactoryTest,
      new CsvReaderTest
    )
