package it.yasp.app.support

import cats.syntax.functor._
import io.circe.generic.auto._
import io.circe.{Decoder, HCursor, KeyDecoder}
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.model.SessionType.{Distributed, Local}
import it.yasp.core.spark.model.{CacheLayer, Dest, Process, SessionType, Source}

import java.util.Locale

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
  implicit def sessionTypeDecoder: Decoder[SessionType] = (c: HCursor) =>
    for {
      value <- c.as[String].map(_.toUpperCase(Locale.US))
    } yield value match {
      case "LOCAL"       => Local
      case "DISTRIBUTED" => Distributed
    }

  /** A CacheLayer circe Decoder
    *
    * Provide a better way to decode the CacheLayer
    *
    * @return
    *   Decoder[CacheLayer]
    */
  implicit def cacheLayerDecoder: Decoder[CacheLayer] = (c: HCursor) =>
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

  /** A Source circe Decoder
    * @return
    *   Decoder[Source]
    */
  implicit def sourceDecoder: Decoder[Source] =
    List[Decoder[Source]](
      Decoder[Source.Format].widen
    ).reduceLeft(_ or _)

  /** A Dest circe Decoder
    * @return
    *   Decoder[Dest]
    */
  implicit def destDecoder: Decoder[Dest] =
    List[Decoder[Dest]](
      Decoder[Dest.Format].widen
    ).reduceLeft(_ or _)

  /** A Process circe Decoder
    * @return
    *   Decoder[Process]
    */
  implicit def processDecoder: Decoder[Process] =
    Decoder[Process.Sql].widen

}
