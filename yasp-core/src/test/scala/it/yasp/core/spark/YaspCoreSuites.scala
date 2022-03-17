package it.yasp.core.spark

import it.yasp.core.spark.processor.SqlProcessorTest
import it.yasp.core.spark.reader._
import it.yasp.core.spark.registry.RegistryTest
import it.yasp.core.spark.session.SparkSessionFactoryTest
import it.yasp.core.spark.writer.ParquetWriterTest
import org.scalatest.Stepwise

class YaspCoreSuites
    extends Stepwise(
      new SparkSessionFactoryTest,
      new CsvReaderTest,
      new JsonReaderTest,
      new ParquetReaderTest,
      new JdbcReaderTest,
      new AvroReaderTest,
      new XmlReaderTest,
      new SqlProcessorTest,
      new ParquetWriterTest,
      new RegistryTest
    )
