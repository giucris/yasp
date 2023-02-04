package it.yasp.service.model

import it.yasp.core.spark.model.Source
import it.yasp.service.model.YaspAction.YaspSource
import org.scalatest.funsuite.AnyFunSuite

import scala.annotation.tailrec

class YaspPlanTest extends AnyFunSuite {

  case class NotADag()
  implicit class YaspPlanOps(yaspPlan: YaspPlan) {

    def sort: Either[NotADag, YaspPlan] = {
      @tailrec
      def topoSort(
          actions: Seq[YaspAction],
          actionsDag: Seq[YaspAction] = Seq.empty
      ): Either[NotADag, Seq[YaspAction]] =
        actions.partition(a => a.dependsOn.map(_ diff actionsDag.map(_.id)).getOrElse(Seq.empty).isEmpty) match {
          case (Nil, Nil)           => Right(actionsDag)
          case (Nil, _ :: _)        => Left(NotADag())
          case (withNoDep, withDep) => topoSort(withDep, actionsDag ++ withNoDep.sortBy(_.id))
        }
      topoSort(yaspPlan.actions).map(YaspPlan)
    }
  }

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
    val actual   = yaspPlan.sort
    val expected = Left(NotADag())
    assert(actual == expected)
  }
}
