package it.yasp.core.spark.model

import it.yasp.core.spark.model.SessionType.{Distributed, Local}
import org.scalatest.funsuite.AnyFunSuite

class SessionTest extends AnyFunSuite {

  test("Local session return local main") {
    val session = Session(Local, "x")
    assert(session.master.contains("local[*]"))
  }

  test("Distributed session return local main") {
    val session = Session(Distributed, "x")
    assert(session.master.isEmpty)
  }
}
