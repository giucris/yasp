package it.yasp.service

import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.{Dest, Source}
import it.yasp.service.YaspService.DefaultYaspService
import it.yasp.service.loader.YaspLoader
import it.yasp.service.model.{YaspPlan, YaspProcess, YaspSink, YaspSource}
import it.yasp.service.processor.YaspProcessor
import it.yasp.service.writer.YaspWriter
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspServiceTest extends AnyFunSuite with MockFactory {

  val loader: YaspLoader       = mock[YaspLoader]
  val processor: YaspProcessor = mock[YaspProcessor]
  val writer: YaspWriter       = mock[YaspWriter]

  val yaspService = new DefaultYaspService(loader, processor, writer)

  test("run with 1 source and 1 sink") {
    inSequence(
      (loader.load _)
        .expects(YaspSource("id1", Source.Json(Seq("sourcePath")), None))
        .once(),
      (writer.write _)
        .expects(YaspSink("id1", Dest.Parquet("destPath")))
        .once()
    )

    yaspService.run(
      YaspPlan(
        sources = Seq(YaspSource("id1", Source.Json(Seq("sourcePath")), None)),
        processes = Seq.empty,
        sinks = Seq(YaspSink("id1", Dest.Parquet("destPath")))
      )
    )
  }

  test("run with 1 source 1 process 1 sink") {
    inSequence(
      (loader.load _)
        .expects(YaspSource("id1", Source.Json(Seq("sourcePath")), None))
        .once(),
      (processor.process _)
        .expects(YaspProcess("id2", Sql("my-sql")))
        .once(),
      (writer.write _)
        .expects(YaspSink("id2", Dest.Parquet("destPath")))
        .once()
    )

    yaspService.run(
      YaspPlan(
        sources = Seq(YaspSource("id1", Source.Json(Seq("sourcePath")), None)),
        processes = Seq(YaspProcess("id2", Sql("my-sql"))),
        sinks = Seq(YaspSink("id2", Dest.Parquet("destPath")))
      )
    )
  }

  test("run with n source n process n sink") {
    inSequence(
      (loader.load _)
        .expects(YaspSource("id1", Source.Json(Seq("sourcePath1")), None))
        .once(),
      (loader.load _)
        .expects(YaspSource("id2", Source.Parquet(Seq("sourcePath2"), mergeSchema = true), None))
        .once(),
      (processor.process _)
        .expects(YaspProcess("id3", Sql("my-sql-1")))
        .once(),
      (processor.process _)
        .expects(YaspProcess("id4", Sql("my-sql-2")))
        .once(),
      (writer.write _)
        .expects(YaspSink("id4", Dest.Parquet("destPath1")))
        .once(),
      (writer.write _)
        .expects(YaspSink("id3", Dest.Parquet("destPath2")))
        .once()
    )

    yaspService.run(
      YaspPlan(
        sources = Seq(
          YaspSource("id1", Source.Json(Seq("sourcePath1")), None),
          YaspSource("id2", Source.Parquet(Seq("sourcePath2"), mergeSchema = true), None)
        ),
        processes = Seq(
          YaspProcess("id3", Sql("my-sql-1")),
          YaspProcess("id4", Sql("my-sql-2"))
        ),
        sinks = Seq(
          YaspSink("id4", Dest.Parquet("destPath1")),
          YaspSink("id3", Dest.Parquet("destPath2"))
        )
      )
    )
  }

}
