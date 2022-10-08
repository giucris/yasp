package it.yasp.app.args

import org.scalatest.funsuite.AnyFunSuite

class YaspArgsTest extends AnyFunSuite {

  test("parse fail") {
    assert(YaspArgs.parse(Array()).isLeft)
  }

  test("parse -f") {
    assert(YaspArgs.parse(Array("-f", "file-path")).contains(YaspArgs("file-path")))
  }

  test("parse --file") {
    assert(YaspArgs.parse(Array("--file", "file-path")).contains(YaspArgs("file-path")))
  }

  test("parse --file --dry-run") {
    val actual = YaspArgs.parse(Array("--file", "file-path", "--dry-run"))
    assert(actual.contains(YaspArgs("file-path", dryRun = true)))
  }
}
