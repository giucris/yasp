package it.yasp.service.model

import cats.implicits._
import it.yasp.service.err.YaspServiceError.YaspPlanError

import scala.annotation.tailrec

/** YaspPlan. Define a Plan model.
  *
  * @param actions:
  *   a [[Seq]] of [[YaspAction]]
  */
final case class YaspPlan(actions: Seq[YaspAction])

object YaspPlan {

  implicit class YaspPlanOps(yaspPlan: YaspPlan) {

    def sort: Either[YaspPlanError, YaspPlan] = {
      @tailrec
      def topoSort(
          actions: Seq[YaspAction],
          actionsDag: Seq[YaspAction] = Seq.empty
      ): Either[Throwable, Seq[YaspAction]] =
        actions.partition(a => a.dependsOn.map(_ diff actionsDag.map(_.id)).getOrElse(Seq.empty).isEmpty) match {
          case (Nil, Nil)           => Right(actionsDag)
          case (Nil, _ :: _)        =>
            Left(new IllegalArgumentException("It seems that the provided actions it's a cyclic graph"))
          case (withNoDep, withDep) => topoSort(withDep, actionsDag ++ withNoDep)
        }
      topoSort(yaspPlan.actions)
        .map(actions => YaspPlan(actions))
        .leftMap(t => YaspPlanError(yaspPlan, t))
    }
  }
}
