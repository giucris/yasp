package it.yasp.core.spark.plugin

import org.scalatest.funsuite.AnyFunSuite

class PluginProviderTest extends AnyFunSuite {

  val pluginProvider = new PluginProvider()

  test("load return right ReaderPlugin") {
    assert(pluginProvider.load[ReaderPlugin]("it.yasp.core.spark.plugin.MyTestReaderPlugin").isRight)
  }

  test("load return right ProcessorPlugin") {
    assert(pluginProvider.load[ProcessorPlugin]("it.yasp.core.spark.plugin.MyTestProcessorPlugin").isRight)
  }

  test("load return right WriterPlugin") {
    assert(pluginProvider.load[WriterPlugin]("it.yasp.core.spark.plugin.MyTestWriterPlugin").isRight)
  }

  test("load return left") {
    assert(pluginProvider.load[ReaderPlugin]("it.yasp.core.spark.plugin.MyTestFailPlugin").isLeft)
  }
}
