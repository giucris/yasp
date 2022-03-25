package it.yasp.app.vars

import it.yasp.app.conf.VariablesSupport
import org.scalatest.funsuite.AnyFunSuite

class VariablesSupportTest extends AnyFunSuite with VariablesSupport {

  test("variable interpolation") {
    val actual   = interpolate(
      "name: ${name}, surname: ${surname}",
      Map("name" -> "tester", "surname" -> "coder")
    )
    val expected = "name: tester, surname: coder"
    assert(actual == expected)
  }

}
