package it.yasp.app.conf

import it.yasp.app.err.YaspAppErrors.ReadFileError
import it.yasp.testkit.TestUtils._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FileSupportTest extends AnyFunSuite with FileSupport with BeforeAndAfterAll {
  private val workspace = "yasp-app/src/test/resources/FileSupportTest"

  override protected def beforeAll(): Unit = {
    cleanFolder(workspace)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    cleanFolder(workspace)
    super.afterAll()
  }

  test("read return Right content") {
    createFile(s"$workspace/testFile.txt", Seq("my file"))
    assert(read(s"$workspace/testFile.txt") == Right("my file"))
  }

  test("read return Left error") {
    val actual = read(s"$workspace/testNotExistingFile.txt")
    assert(actual.isLeft)
    assert(actual.left.get.isInstanceOf[ReadFileError])
  }
}
