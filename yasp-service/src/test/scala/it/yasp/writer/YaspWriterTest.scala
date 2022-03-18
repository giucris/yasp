package it.yasp.writer

import it.yasp.core.spark.model.Dest
import it.yasp.core.spark.model.Dest._
import org.scalatest.funsuite.AnyFunSuite

class YaspWriterTest extends AnyFunSuite {

  test("x") {
    //new YaspWriter().write(YaspSink("my_table",Parquet("my_path")))
  }

}

case class YaspSink(id:String,dest:Dest)
