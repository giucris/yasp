package it.yasp.service.model

import it.yasp.core.spark.model.Dest

/** YaspSink model
  * @param id:
  *   a [[String]] id of the data to store on the specific Dest
  * @param dest:
  *   a [[Dest]] instance
  */
final case class YaspSink(id: String, dest: Dest)
