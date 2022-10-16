package it.yasp.core.spark.plugin

import com.typesafe.scalalogging.StrictLogging

import java.util.ServiceLoader
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.reflect.{classTag, ClassTag}

class PluginProvider extends StrictLogging {

  /** Load clazz instance via ServiceLoader
    * @param clazz:
    *   String
    * @tparam A:
    *   Class
    * @return
    *   Right(A) if the ServiceLoader successful retrieve the instance Left(Throwable) otherwise
    */
  def load[A: ClassTag](clazz: String): Either[Throwable, A] = {
    logger.info(s"Loading class: $clazz")
    try ServiceLoader
      .load(classTag[A].runtimeClass.asInstanceOf[Class[A]])
      .iterator()
      .toSeq
      .find(_.getClass.getCanonicalName == clazz)
      .toRight(new ClassNotFoundException(clazz))
    catch { case t: Throwable => Left(t) }
  }
}
