package it.yasp.core.spark.testutils

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.reflect.io.Path

object TestUtils {

  def cleanFolder(path: String): Unit =
    Path(path).deleteRecursively()

  def createFile(filePath: String, rows: Seq[String]): Unit = {
    Files.createDirectories(Paths.get(filePath).getParent)
    Files.write(Files.createFile(Paths.get(filePath)), rows.asJava)
  }

}
