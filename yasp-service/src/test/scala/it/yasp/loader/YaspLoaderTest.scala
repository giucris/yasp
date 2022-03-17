package it.yasp.loader

import it.yasp.core.spark.model.Source
import it.yasp.core.spark.reader.Reader
import it.yasp.core.spark.registry.Registry
import it.yasp.model.YaspSource
import it.yasp.testkit.SparkTestSuite
import org.scalatest.funsuite.AnyFunSuite

class YaspLoaderTest extends AnyFunSuite with SparkTestSuite {

  test("load"){
    val df =
  }
}

trait YaspLoader{
  def load(source:YaspSource):Unit
}

object YaspLoader {

  class DefaultYaspLoader(reader:Reader[Source],registry: Registry) extends YaspLoader {
    override def load(source: YaspSource): Unit = {
      val src = reader.read(source.source)
      registry.register(src,source.id)
    }
  }
}