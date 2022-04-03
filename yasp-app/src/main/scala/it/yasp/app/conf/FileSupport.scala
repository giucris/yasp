package it.yasp.app.conf

import scala.io.Source

trait FileSupport {

  //TODO remove the throw in next feature with sealed error handling
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def read(filePath: String): String = {
    val source = Source.fromFile(filePath,"UTF-8")
    try source.mkString.trim
    catch {
      case e: Exception => throw e
    } finally source.close()
  }

}
