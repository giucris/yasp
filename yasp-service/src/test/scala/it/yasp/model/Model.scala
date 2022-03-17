package it.yasp.model

import it.yasp.core.spark.model.{Dest, Source,Process}

trait Model {

  case class YaspProcess(id:String,process:Process)
  case class YaspSink(id:String,dest:Dest)
}
