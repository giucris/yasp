package it.yasp.service.model

/** YaspPlan. Define a Plan model.
  * @param sources:
  *   a [[Seq]] of [[YaspSource]]
  * @param processes:
  *   a [[Seq]] of [[YaspProcess]]
  * @param sinks:
  *   a [[Seq]] of [[YaspSink]]
  */
final case class YaspPlan(
    sources: Seq[YaspSource] = Seq.empty,
    processes: Seq[YaspProcess] = Seq.empty,
    sinks: Seq[YaspSink] = Seq.empty
)
