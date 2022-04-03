package it.yasp.core.spark.writer

import it.yasp.core.spark.model.Dest.Csv
import it.yasp.core.spark.model.OutFormat
import org.scalatest.funsuite.AnyFunSuite

class WritableTest extends AnyFunSuite {

  Seq(
    (
      Csv("p1", Seq.empty, Map.empty),
      OutFormat("csv", Map("path" -> "p1"), Seq.empty)
    ),
    (
      Csv("p2", Seq("x", "y"), Map.empty),
      OutFormat("csv", Map("path" -> "p2"), Seq("x", "y"))
    ),
    (
      Csv("p3", Seq("x", "y"), Map("k" -> "v")),
      OutFormat("csv", Map("path" -> "p3", "k" -> "v"), Seq("x", "y"))
    ),
    (
      Csv("p4", Seq("x", "y"), Map("k" -> "v", "path" -> "z")),
      OutFormat("csv", Map("path" -> "p4", "k" -> "v"), Seq("x", "y"))
    )
  ) foreach { case (input: Csv, expected: OutFormat) =>
    test(s"csv writable format $input") {
      assert(Writable.csvWritable.format(input) == expected)
    }
  }

}
