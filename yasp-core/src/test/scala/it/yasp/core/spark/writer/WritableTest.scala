package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Csv
import it.yasp.core.spark.model.OutFormat
import org.scalatest.funsuite.AnyFunSuite

class WritableTest extends AnyFunSuite {

  test("csvWritable create OutFormat") {
    val actual   = Writable.csvWritable.format(Csv("p1", Seq.empty, Map.empty))
    val expected = OutFormat("csv", Map("path" -> "p1"), Seq.empty)
    assert(actual == expected)
  }

  test("csvWritable create OutFormat with path override") {
    val actual   = Writable.csvWritable.format(Csv("p1", Seq.empty, Map("path"->"px")))
    val expected = OutFormat("csv", Map("path" -> "p1"), Seq.empty)
    assert(actual == expected)
  }

  test("csvWritable create OutFormat with partitionBy and options") {
    val actual   = Writable.csvWritable.format(Csv("p2", Seq("x", "y"), Map("k" -> "v")))
    val expected = OutFormat("csv", Map("path" -> "p2", "k" -> "v"), Seq("x", "y"))
    assert(actual == expected)
  }

}
