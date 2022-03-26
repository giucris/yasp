package it.yasp.app.conf

import scala.io.Source

trait FileSupport {

  def read(filePath: String): String = {
    val source = Source.fromFile(filePath)
    try source.mkString.trim
    catch {
      case e: Exception => throw e
    } finally source.close()
  }

}
