package it.yasp.model

import it.yasp.core.spark.session.SessionConf

case class YaspExecution(sessionConf: SessionConf, plan: YaspPlan)
