package it.yasp.service.resolver

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.yasp.service.err.YaspServiceError.YaspResolveError
import it.yasp.service.model.{YaspAction, YaspPlan}

import scala.annotation.tailrec

/** YaspResolver instance.
  *
  * It will take care to resolve the YaspPlan.
  *
  * Currently it only resolve the dependency between action provided by the User.
  */
class YaspResolver extends StrictLogging {

  /** Resolve a [[YaspPlan]]
    * @param yaspPlan:
    *   An input [[YaspPlan]]
    * @return
    *   A new [[YaspPlan]] with all actions in provided order
    */
  def resolve(yaspPlan: YaspPlan): Either[YaspResolveError, YaspPlan] = {
    logger.info(s"Resolving YaspPlan dependency")
    topologicalSort(yaspPlan.actions).map(YaspPlan).leftMap(t => YaspResolveError(yaspPlan, t))
  }

  @tailrec
  private def topologicalSort(
      actions: Seq[YaspAction],
      actionsDag: Seq[YaspAction] = Seq.empty
  ): Either[Throwable, Seq[YaspAction]] =
    actions.partition(a => a.dependsOn.map(_ diff actionsDag.map(_.id)).getOrElse(Seq.empty).isEmpty) match {
      case (Nil, Nil)           => Right(actionsDag)
      case (Nil, _ :: _)        => Left(new IllegalArgumentException("The provided actions is not a DAG"))
      case (withNoDep, withDep) => topologicalSort(withDep, actionsDag ++ withNoDep)
    }

}
