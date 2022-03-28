package it.yasp.service

import it.yasp.service.executor.YaspExecutor.DefaultYaspExecutor
import it.yasp.service.executor.YaspExecutorFactory
import it.yasp.testkit.SparkTestSuite
import org.scalatest.funsuite.AnyFunSuite

class YaspExecutorFactoryTest extends AnyFunSuite with SparkTestSuite {

  test("create") {
    val actual = new YaspExecutorFactory().create(spark)
    assert(actual.isInstanceOf[DefaultYaspExecutor])
  }

}
