package it.yasp.core.spark.session

import it.yasp.core.spark.session.SessionType.{Distributed, Local}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class SparkSessionFactoryTest extends AnyFunSuite {

  val sessionFactory = new SparkSessionFactory()

  test("create local session") {
    val session = sessionFactory.create(
      SessionConf(
        sessionType = Local,
        appName = "test-app-1",
        config = Map.empty
      )
    )

    assert(session.isInstanceOf[SparkSession])
    assert(session.sparkContext.master == "local[*]")
    assert(session.sparkContext.appName == "test-app-1")

    session.stop()
  }

  test("create local session with config") {
    val session = sessionFactory.create(
      SessionConf(
        sessionType = Local,
        appName = "test-app-2",
        config = Map("myConf1" -> "myConfValue1", "myConf2" -> "myConfValue2")
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
        SessionConf(
          sessionType = Distributed,
          appName = "test-app-1",
          config = Map("myConf1" -> "myConfValue1", "myConf2" -> "myConfValue2")
        )
      )
      spark.stop()
    }

    val expectedExMessage = "A master URL must be set in your configuration"
    assert(exception.getMessage == expectedExMessage)
  }
}
