package it.yasp.app.conf

import org.apache.commons.text.StringSubstitutor

import scala.collection.JavaConverters.mapAsJavaMapConverter

trait VariablesSupport {
  def interpolate(value: String, valueMap: Map[String, String]): String =
    new StringSubstitutor(valueMap.asJava).replace(value)
}
