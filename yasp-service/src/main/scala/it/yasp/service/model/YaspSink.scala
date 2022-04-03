package it.yasp.service.model

import it.yasp.core.spark.model.Dest

/** Define a YaspSink
  * @param id:
  *   the id of the data to write on the [[Dest]]
  * @param dest:
  *   An instance of [[Dest]]
  */
final case class YaspSink(id: String, dest: Dest)
