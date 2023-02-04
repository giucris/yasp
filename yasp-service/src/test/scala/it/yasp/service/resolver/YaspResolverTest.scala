package it.yasp.service.resolver

import it.yasp.core.spark.model.Source
import it.yasp.service.model.YaspAction.YaspSource
import it.yasp.service.model.YaspPlan
import it.yasp.testkit.SparkTestSuite
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class YaspResolverTest extends AnyFunSuite with SparkTestSuite with MockFactory with BeforeAndAfterAll {

  test("sort simple dependsOn return Right") {
    val actual   = new YaspResolver().resolve(
      YaspPlan(
        Seq(
          YaspSource("2", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
          YaspSource("1", "ds2", Source.Format("x", None), None, None, None)
        )
      )
    )
    val expected = Right(
      YaspPlan(
        Seq(
          YaspSource("1", "ds2", Source.Format("x", None), None, None, None),
          YaspSource("2", "ds2", Source.Format("x", None), None, None, Some(Seq("1")))
        )
      )
    )
    assert(actual == expected)
  }

  test("sort complex dependsOn return Right") {
    val actual = new YaspResolver().resolve(
      YaspPlan(
        Seq(
          YaspSource("2", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
          YaspSource("5", "ds2", Source.Format("x", None), None, None, Some(Seq("3", "4"))),
          YaspSource("4", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
          YaspSource("3", "ds2", Source.Format("x", None), None, None, Some(Seq("2", "1"))),
          YaspSource("1", "ds2", Source.Format("x", None), None, None, None)
        )
      )
    )

    val expected = Right(
      YaspPlan(
        Seq(
          YaspSource("1", "ds2", Source.Format("x", None), None, None, None),
          YaspSource("2", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
          YaspSource("4", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
          YaspSource("3", "ds2", Source.Format("x", None), None, None, Some(Seq("2", "1"))),
          YaspSource("5", "ds2", Source.Format("x", None), None, None, Some(Seq("3", "4")))
        )
      )
    )
    assert(actual == expected)
  }

  test("sort return Left if plan is not a DAG") {
    val actual = new YaspResolver().resolve(
      YaspPlan(
        Seq(
          YaspSource("2", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
          YaspSource("3", "ds2", Source.Format("x", None), None, None, Some(Seq("2"))),
          YaspSource("1", "ds2", Source.Format("x", None), None, None, Some(Seq("3")))
        )
      )
    )
    assert(actual.isLeft)
  }
}
