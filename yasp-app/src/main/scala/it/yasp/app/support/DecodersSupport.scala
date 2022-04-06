package it.yasp.app.support

import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor, KeyDecoder}
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.model.SessionType.{Distributed, Local}
import it.yasp.core.spark.model.{CacheLayer, SessionType}

/** Provide a set of encoder and decoder useful to beautify all Yasp ADT.
  */
trait DecodersSupport {

  /** Rewrite the default decodeSeq.
    *
    * In yasp any Map should be parsed as empty even if null
    * @tparam A:
    *   type of Seq element
    * @return
    */
  implicit def decodeSeq[A: Decoder]: Decoder[Seq[A]] =
    Decoder.decodeOption(Decoder.decodeSeq[A]).map(_.toSeq.flatten)

  /** Rewrite the default decodeMap.
    *
    * In yasp any Map should be parsed as empty even if null
    * @tparam A:
    *   Key type
    * @tparam B:
    *   Value type
    * @return
    */
  implicit def decodeMap[A: KeyDecoder, B: Decoder]: Decoder[Map[A, B]] =
    Decoder.decodeOption(Decoder.decodeMap[A, B]).map(_.getOrElse(Map.empty))

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
