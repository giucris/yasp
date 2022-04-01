package it.yasp.service.model

final case class YaspPlan(
    sources: Seq[YaspSource] = Seq.empty,
    processes: Seq[YaspProcess] = Seq.empty,
    sinks: Seq[YaspSink] = Seq.empty
)
