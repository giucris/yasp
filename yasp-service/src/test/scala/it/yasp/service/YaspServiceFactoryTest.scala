package it.yasp.service

import it.yasp.service.YaspService.DefaultYaspService
import it.yasp.testkit.SparkTestSuite
import org.scalatest.funsuite.AnyFunSuite

class YaspServiceFactoryTest extends AnyFunSuite with SparkTestSuite {

  test("create") {
    val actual = new YaspServiceFactory().create(spark)
    assert(actual.isInstanceOf[DefaultYaspService])
  }

}
