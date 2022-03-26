package it.yasp.app.conf

import it.yasp.testkit.TestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FileSupportTest extends AnyFunSuite with FileSupport with BeforeAndAfterAll {
  private val workspace = "yasp-app/src/test/resources/FileSupportTest"

  override protected def beforeAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    TestUtils.cleanFolder(workspace)
    super.afterAll()
  }
  test("read") {
    TestUtils.createFile(s"$workspace/testFile.txt", Seq("my file"))
    val actual = read(s"$workspace/testFile.txt")
    assert(actual == "my file")
  }
}
