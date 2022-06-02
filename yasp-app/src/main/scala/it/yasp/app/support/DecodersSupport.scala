package it.yasp.app.support

import cats.syntax.functor._
import io.circe.Decoder.Result
import io.circe.generic.auto._
import io.circe.{Decoder, HCursor, KeyDecoder}
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.model.SessionType.{Distributed, Local}
import it.yasp.core.spark.model.{CacheLayer, Dest, Process, SessionType, Source}

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

  /** A Source circe Decoder
    * @return
    *   Decoder[Source]
    */
  implicit def sourceDecoder: Decoder[Source] =
    List[Decoder[Source]](
      Decoder[Source.Csv].widen,
      Decoder[Source.Json].widen,
      Decoder[Source.Avro].widen,
      Decoder[Source.Xml].widen,
      Decoder[Source.Orc].widen,
      Decoder[Source.Parquet].widen,
      Decoder[Source.Jdbc].widen
    ).reduceLeft(_ or _)

  /** A Dest circe Decoder
    * @return
    *   Decoder[Dest]
    */
  implicit def destDecoder: Decoder[Dest] =
    Decoder[Dest.Parquet].widen

  /** A Process circe Decoder
    * @return
    *   Decoder[Process]
    */
  implicit def processDecoder: Decoder[Process] =
    Decoder[Process.Sql].widen

}
