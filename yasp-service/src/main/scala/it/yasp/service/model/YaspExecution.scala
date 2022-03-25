package it.yasp.service.model

import it.yasp.core.spark.session.SessionConf

case class YaspExecution(conf: SessionConf, plan: YaspPlan)
