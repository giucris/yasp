package it.yasp.app.conf

import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor}
import it.yasp.core.spark.model.CacheLayer
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.session.SessionType
import it.yasp.core.spark.session.SessionType.{Distributed, Local}

/** Provide a set of encoder and decoder useful to beautify all Yasp ADT.
  */
trait DecodersSupport {

  /** A SessionType circe Decoder
    *
    * Provide a better way to decode the SessionType ADT.
    *
    * @return
    *   Decoder[SessionType]
    */
  implicit def sessionTypeDecoder: Decoder[SessionType] = new Decoder[SessionType] {
    override def apply(c: HCursor): Result[SessionType] =
      for {
        value <- c.as[String].right
      } yield value match {
        case "Local"       => Local
        case "Distributed" => Distributed
      }
  }

  /** A CacheLayer circe Decoder
    *
    * Provide a better way to decode the CacheLayer
    *
    * @return
    *   Decoder[CacheLayer]
    */
  implicit def cacheLayerDecoder: Decoder[CacheLayer] = new Decoder[CacheLayer] {
    override def apply(c: HCursor): Result[CacheLayer] =
      for {
        value <- c.as[String].right
      } yield value match {
        case "Memory"           => Memory
        case "Disk"             => Disk
        case "MemoryAndDisk"    => MemoryAndDisk
        case "MemorySer"        => MemorySer
        case "MemoryAndDiskSer" => MemoryAndDiskSer
        case "Checkpoint"       => Checkpoint
      }
  }

}
