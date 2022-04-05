package it.yasp.app.support

import it.yasp.app.err.YaspAppErrors.InterpolationError
import org.scalatest.funsuite.AnyFunSuite

class VariablesSupportTest extends AnyFunSuite with VariablesSupport {

  test("interpolate return Right with content replaced") {
    val actual   = interpolate(
      value = "name: ${name}, surname: ${surname}",
      values = Map("name" -> "tester", "surname" -> "coder")
    )
    val expected = "name: tester, surname: coder"
    assert(actual == Right(expected))
  }

  test("interpolate return Left with error") {
    val actual = interpolate(
      value = "name: ${name}, surname: ${surname}",
      values = Map("name" -> "tester")
    )
    assert(actual.isLeft)
    assert(actual.left.get.isInstanceOf[InterpolationError])
  }
}
