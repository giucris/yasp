package it.yasp.core.spark.plugin

import org.scalatest.funsuite.AnyFunSuite

class PluginProviderTest extends AnyFunSuite {

  val pluginProvider = new PluginProvider()

  test("load return right") {
    val actual = pluginProvider.load[ReaderPlugin]("it.yasp.core.spark.plugin.MyTestReaderPlugin")
    assert(actual.isRight)
  }

  test("load return left") {
    val actual = pluginProvider.load[ReaderPlugin]("it.yasp.core.spark.plugin.MyTestFailPlugin")
    assert(actual.isLeft)
  }
}
