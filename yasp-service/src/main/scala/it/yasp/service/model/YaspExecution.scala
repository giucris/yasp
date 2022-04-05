package it.yasp.service.model

import it.yasp.core.spark.model.Session

/** YaspExecution.
  *
  * Define an execution model for the Yasp framework.
  * @param session:
  *   A [[Session]] instance
  * @param plan:
  *   A [[YaspPlan]] instance
  */
final case class YaspExecution(
    session: Session,
    plan: YaspPlan
)
