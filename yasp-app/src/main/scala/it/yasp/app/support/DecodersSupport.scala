package it.yasp.app.support

import cats.syntax.functor._
import io.circe.generic.auto._
import io.circe.{Decoder, HCursor, KeyDecoder}
import it.yasp.core.spark.model.CacheLayer._
import it.yasp.core.spark.model.IcebergCatalog.{CustomIcebergCatalog, HadoopIcebergCatalog, HiveIcebergCatalog}
import it.yasp.core.spark.model.SessionType.{Distributed, Local}
import it.yasp.core.spark.model.{CacheLayer, Dest, IcebergCatalog, Process, SessionType, Source}
import it.yasp.service.model.YaspAction
import it.yasp.service.model.YaspAction.{YaspProcess, YaspSink, YaspSource}

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

  implicit def yaspActionDecoder: Decoder[YaspAction] =
    List[Decoder[YaspAction]](
      Decoder[YaspSource].widen,
      Decoder[YaspProcess].widen,
      Decoder[YaspSink].widen
    ).reduceLeft(_ or _)

  /** A CacheLayer circe Decoder
    *
    * Provide a better way to decode the CacheLayer
    *
    * @return
    *   Decoder[CacheLayer]
    */
  implicit def cacheLayerDecoder: Decoder[CacheLayer] = (c: HCursor) =>
    for {
      value <- c.as[String].right.map(_.toUpperCase(Locale.US))
    } yield value match {
      case "MEMORY"                                   => Memory
      case "DISK"                                     => Disk
      case "MEMORYANDDISK" | "MEMORY_AND_DISK"        => MemoryAndDisk
      case "MEMORYSER" | "MEMORY_SER"                 => MemorySer
      case "MEMORYANDDISKSER" | "MEMORY_AND_DISK_SER" => MemoryAndDiskSer
      case "CHECKPOINT"                               => Checkpoint
    }

  /** A Source circe Decoder
    * @return
    *   Decoder[Source]
    */
  implicit def sourceDecoder: Decoder[Source] =
    List[Decoder[Source]](
      Decoder[Source.Format].widen,
      Decoder[Source.Custom].widen,
      Decoder[Source.HiveTable].widen
    ).reduceLeft(_ or _)

  /** A Dest circe Decoder
    * @return
    *   Decoder[Dest]
    */
  implicit def destDecoder: Decoder[Dest] =
    List[Decoder[Dest]](
      Decoder[Dest.Format].widen,
      Decoder[Dest.Custom].widen,
      Decoder[Dest.HiveTable].widen
    ).reduceLeft(_ or _)

  /** A Process circe Decoder
    * @return
    *   Decoder[Process]
    */
  implicit def processDecoder: Decoder[Process] =
    List[Decoder[Process]](
      Decoder[Process.Sql].widen,
      Decoder[Process.Custom].widen
    ).reduceLeft(_ or _)

  /** An IcebergCatalog circe Decoder
    * @return
    *   Decoder[IcebergCatalog]
    */
  implicit def icebergCatalogDecoder: Decoder[IcebergCatalog] =
    List[Decoder[IcebergCatalog]](
      Decoder[HiveIcebergCatalog].widen,
      Decoder[HadoopIcebergCatalog].widen,
      Decoder[CustomIcebergCatalog].widen
    ).reduceLeft(_ or _)

}
