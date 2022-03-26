package it.yasp.service.model

case class YaspPlan(
    sources: Seq[YaspSource],
    processes: Seq[YaspProcess],
    sinks: Seq[YaspSink]
)
