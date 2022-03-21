package it.yasp.model

case class YaspPlan(
    sources: Seq[YaspSource],
    processes: Seq[YaspProcess],
    sinks: Seq[YaspSink]
)
