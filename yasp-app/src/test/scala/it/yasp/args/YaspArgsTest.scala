package it.yasp.args

import org.scalatest.funsuite.AnyFunSuite

class YaspArgsTest extends AnyFunSuite {

  test("parse fail") {
    assert(YaspArgs.parse(Array()).contains(YaspArgs()))
  }

  test("parse -f") {
    assert(YaspArgs.parse(Array("-f", "file-path")).contains(YaspArgs("file-path")))
  }

  test("parse --file") {
    assert(YaspArgs.parse(Array("--file", "file-path")).contains(YaspArgs("file-path")))
  }

}
