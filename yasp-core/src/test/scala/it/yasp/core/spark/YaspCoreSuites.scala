package it.yasp.core.spark

import it.yasp.core.spark.reader._
import it.yasp.core.spark.session.SparkSessionFactoryTest
import org.scalatest.Stepwise

class YaspCoreSuites
    extends Stepwise(
      new SparkSessionFactoryTest,
      new CsvDataSourceReaderTest,
      new JsonDataSourceReaderTest,
      new ParquetDataSourceReaderTest,
      new JdbcDataSourceReaderTest,
      new AvroDataSourceReaderTest,
      new XmlDataSourceReaderTest
    )
