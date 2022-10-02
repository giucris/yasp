package it.yasp.app.support

import it.yasp.app.err.YaspError.ReadFileError
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

  test("read return Left error, file not readable") {
    createFile(s"$workspace/testFile2.txt", Seq.empty)
    assert(read(s"$workspace/testFile2.txt") == Right(""))
  }

  test("read return Left error, file not found") {
    val actual = read(s"$workspace/testFile3.txt")
    assert(actual.isLeft)
    assert(actual.left.get.isInstanceOf[ReadFileError])
  }
}
