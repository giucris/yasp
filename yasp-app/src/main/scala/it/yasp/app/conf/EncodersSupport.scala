package it.yasp.app.conf

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import it.yasp.core.spark.model.CacheLayer
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.session.SessionType
import it.yasp.core.spark.session.SessionType.{Distributed, Local}

/** Provide a set of encoder and decoder useful to beautify all Yasp ADT.
  */
trait EncodersSupport {

  /** A SessionType circe Encoder.
    *
    * Provide a better way to encode the SessionType ADT.
    *
    * @return
    *   Encoder[SessionType]
    */
  implicit def sessionTypeEncoder: Encoder[SessionType] = new Encoder[SessionType] {
    override def apply(a: SessionType): Json =
      a match {
        case SessionType.Local       => Json.fromString("Local")
        case SessionType.Distributed => Json.fromString("Distributed")
      }
  }

  /** A CacheLayer circe Encoder
    *
    * Provide a better way to encode the CacheLayer ADT.
    *
    * @return
    *   Encoder[CacheLayer]
    */
  implicit def cacheLayerEncoder: Encoder[CacheLayer] = new Encoder[CacheLayer] {
    override def apply(a: CacheLayer): Json =
      a match {
        case CacheLayer.Memory           => Json.fromString("Memory")
        case CacheLayer.Disk             => Json.fromString("Disk")
        case CacheLayer.MemoryAndDisk    => Json.fromString("MemoryAndDisk")
        case CacheLayer.MemorySer        => Json.fromString("MemorySer")
        case CacheLayer.MemoryAndDiskSer => Json.fromString("MemoryAndDiskSer")
        case CacheLayer.Checkpoint       => Json.fromString("Checkpoint")
      }
  }

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
