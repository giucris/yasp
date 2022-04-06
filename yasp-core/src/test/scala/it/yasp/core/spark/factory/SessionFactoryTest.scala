package it.yasp.core.spark.factory

import it.yasp.core.spark.model.SessionType.{Distributed, Local}
import it.yasp.core.spark.model._
import org.apache.spark.sql.SparkSession
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class SessionFactoryTest extends AnyFunSuite {

  val sessionFactory = new SessionFactory()

  test("create local session") {
    val session = sessionFactory.create(
      Session(
        kind = Local,
        name = "test-app-1",
        conf = Map.empty
      )
    )

    assert(session.isInstanceOf[SparkSession])
    assert(session.sparkContext.master == "local[*]")
    assert(session.sparkContext.appName == "test-app-1")

    session.stop()
  }

  test("create local session with config") {
    val session = sessionFactory.create(
      Session(
        kind = Local,
        name = "test-app-2",
        conf = Map("myConf1" -> "myConfValue1", "myConf2" -> "myConfValue2")
      )
    )

    assert(session.isInstanceOf[SparkSession])
    assert(session.sparkContext.master == "local[*]")
    assert(session.sparkContext.appName == "test-app-2")
    assert(session.sparkContext.getConf.get("myConf1") == "myConfValue1")
    assert(session.sparkContext.getConf.get("myConf2") == "myConfValue2")

    session.stop()
  }

  test("create distributed session raise a 'master must be set...' exception") {
    val exception = intercept[Exception] {
      val spark = sessionFactory.create(
        Session(
          kind = Distributed,
          name = "test-app-1",
          conf = Map("myConf1" -> "myConfValue1", "myConf2" -> "myConfValue2")
        )
      )
      spark.stop()
    }

    val expectedExMessage = "A master URL must be set in your configuration"
    assert(exception.getMessage == expectedExMessage)
  }
}
