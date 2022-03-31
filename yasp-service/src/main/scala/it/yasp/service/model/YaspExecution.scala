package it.yasp.service.model

import it.yasp.core.spark.model.Session

case class YaspExecution(session: Session, plan: YaspPlan)
