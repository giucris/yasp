package it.yasp.service.model

/** YaspPlan. Define a Plan model.
  * @param actions:
  *   a [[Seq]] of [[YaspAction]]
  */
final case class YaspPlan(actions: Seq[YaspAction])
