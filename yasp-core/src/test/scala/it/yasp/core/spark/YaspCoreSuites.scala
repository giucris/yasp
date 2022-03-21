package it.yasp.core.spark

import it.yasp.core.spark.processor.{ProcessProcessorTest, SqlProcessorTest}
import it.yasp.core.spark.reader._
import it.yasp.core.spark.registry.RegistryTest
import it.yasp.core.spark.session.SparkSessionFactoryTest
import it.yasp.core.spark.writer.{DestWriterTest, ParquetWriterTest}
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
      new SourceReaderTest,
      new SqlProcessorTest,
      new ProcessProcessorTest,
      new ParquetWriterTest,
      new DestWriterTest,
      new RegistryTest
    )
