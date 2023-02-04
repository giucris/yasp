package it.yasp.service.model

import it.yasp.core.spark.model.Source
import it.yasp.service.model.YaspAction.YaspSource
import org.scalatest.funsuite.AnyFunSuite

class YaspPlanTest extends AnyFunSuite {

  test("sort simple dependsOn return Right") {
    val yaspPlan = YaspPlan(
      Seq(
        YaspSource("2", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
        YaspSource("1", "ds2", Source.Format("x", None), None, None, None)
      )
    )
    val actual   = yaspPlan.sort
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
    val yaspPlan = YaspPlan(
      Seq(
        YaspSource("2", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
        YaspSource("5", "ds2", Source.Format("x", None), None, None, Some(Seq("3", "4"))),
        YaspSource("4", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
        YaspSource("3", "ds2", Source.Format("x", None), None, None, Some(Seq("2", "1"))),
        YaspSource("1", "ds2", Source.Format("x", None), None, None, None)
      )
    )
    val actual   = yaspPlan.sort
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
    val yaspPlan = YaspPlan(
      Seq(
        YaspSource("2", "ds2", Source.Format("x", None), None, None, Some(Seq("1"))),
        YaspSource("3", "ds2", Source.Format("x", None), None, None, Some(Seq("2"))),
        YaspSource("1", "ds2", Source.Format("x", None), None, None, Some(Seq("3")))
      )
    )
    assert(yaspPlan.sort.isLeft)
  }
}
