package it.yasp.service

import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.{Dest, Source}
import it.yasp.service.executor.YaspExecutor.DefaultYaspExecutor
import it.yasp.service.loader.YaspLoader
import it.yasp.service.model.{YaspPlan, YaspProcess, YaspSink, YaspSource}
import it.yasp.service.processor.YaspProcessor
import it.yasp.service.writer.YaspWriter
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class YaspExecutorTest extends AnyFunSuite with MockFactory {

  val loader: YaspLoader       = mock[YaspLoader]
  val processor: YaspProcessor = mock[YaspProcessor]
  val writer: YaspWriter       = mock[YaspWriter]

  val yaspExecutor = new DefaultYaspExecutor(loader, processor, writer)

  test("exec with 1 source and 1 sink") {
    inSequence(
      (loader.load _)
        .expects(YaspSource("id1", Source.Json("sourcePath", None), None))
        .once(),
      (writer.write _)
        .expects(YaspSink("id1", Dest.Parquet("destPath",None)))
        .once()
    )

    yaspExecutor.exec(
      YaspPlan(
        sources = Seq(YaspSource("id1", Source.Json("sourcePath", None), None)),
        processes = Seq.empty,
        sinks = Seq(YaspSink("id1", Dest.Parquet("destPath",None)))
      )
    )
  }

  test("exec with 1 source 1 process 1 sink") {
    inSequence(
      (loader.load _)
        .expects(YaspSource("id1", Source.Json("sourcePath", None), None))
        .once(),
      (processor.process _)
        .expects(YaspProcess("id2", Sql("my-sql"), None))
        .once(),
      (writer.write _)
        .expects(YaspSink("id2", Dest.Parquet("destPath",None)))
        .once()
    )

    yaspExecutor.exec(
      YaspPlan(
        sources = Seq(YaspSource("id1", Source.Json("sourcePath", None), None)),
        processes = Seq(YaspProcess("id2", Sql("my-sql"), None)),
        sinks = Seq(YaspSink("id2", Dest.Parquet("destPath",None)))
      )
    )
  }

  test("exec with n source n process n sink") {
    inSequence(
      (loader.load _)
        .expects(YaspSource("id1", Source.Json("sourcePath1", None), None))
        .once(),
      (loader.load _)
        .expects(YaspSource("id2", Source.Parquet("sourcePath2", mergeSchema = true), None))
        .once(),
      (processor.process _)
        .expects(YaspProcess("id3", Sql("my-sql-1"), None))
        .once(),
      (processor.process _)
        .expects(YaspProcess("id4", Sql("my-sql-2"), None))
        .once(),
      (writer.write _)
        .expects(YaspSink("id4", Dest.Parquet("destPath1",None)))
        .once(),
      (writer.write _)
        .expects(YaspSink("id3", Dest.Parquet("destPath2",None)))
        .once()
    )

    yaspExecutor.exec(
      YaspPlan(
        sources = Seq(
          YaspSource("id1", Source.Json("sourcePath1", None), None),
          YaspSource("id2", Source.Parquet("sourcePath2", mergeSchema = true), None)
        ),
        processes = Seq(
          YaspProcess("id3", Sql("my-sql-1"), None),
          YaspProcess("id4", Sql("my-sql-2"), None)
        ),
        sinks = Seq(
          YaspSink("id4", Dest.Parquet("destPath1",None)),
          YaspSink("id3", Dest.Parquet("destPath2",None))
        )
      )
    )
  }

}
