package it.yasp.service.executor

import it.yasp.core.spark.model.Dest.Format
import it.yasp.core.spark.model.Process.Sql
import it.yasp.core.spark.model.Source
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
      Seq(
        (loader.load _)
          .expects(
            YaspSource(
              id = "id1",
              source = Source.Format("json", options = Map("path" -> "sourcePath"))
            )
          )
          .once()
          .returns(Right(())),
        (writer.write _)
          .expects(YaspSink("id1", Format("parquet", Map("url" -> "destPath"))))
          .once()
          .returns(Right(()))
      )
    )

    yaspExecutor.exec(
      YaspPlan(
        sources = Seq(
          YaspSource(
            id = "id1",
            source = Source.Format("json", options = Map("path" -> "sourcePath"))
          )
        ),
        processes = Seq.empty,
        sinks = Seq(YaspSink("id1", Format("parquet", Map("url" -> "destPath"))))
      )
    )
  }

  test("exec with 1 source 1 process 1 sink") {
    inSequence(
      Seq(
        (loader.load _)
          .expects(
            YaspSource(
              id = "id1",
              source = Source.Format("json", options = Map("path" -> "sourcePath"))
            )
          )
          .once()
          .returns(Right(())),
        (processor.process _)
          .expects(YaspProcess("id2", Sql("my-sql"), None))
          .once()
          .returns(Right(())),
        (writer.write _)
          .expects(YaspSink("id2", Format("parquet", Map("url" -> "destPath"))))
          .once()
          .returns(Right(()))
      )
    )

    yaspExecutor.exec(
      YaspPlan(
        sources = Seq(
          YaspSource(
            id = "id1",
            source = Source.Format("json", options = Map("path" -> "sourcePath"))
          )
        ),
        processes = Seq(YaspProcess("id2", Sql("my-sql"), None)),
        sinks = Seq(YaspSink("id2", Format("parquet", Map("url" -> "destPath"))))
      )
    )
  }

  test("exec with n source n process n sink") {
    inSequence(
      Seq(
        (loader.load _)
          .expects(
            YaspSource(
              id = "id1",
              source = Source.Format("json", options = Map("path" -> "sourcePath1"))
            )
          )
          .once()
          .returns(Right(())),
        (loader.load _)
          .expects(
            YaspSource(
              id = "id2",
              source = Source.Format("parquet", options = Map("path" -> "sourcePath2", "mergeSchema" -> "true"))
            )
          )
          .once()
          .returns(Right(())),
        (processor.process _)
          .expects(YaspProcess("id3", Sql("my-sql-1"), None))
          .once()
          .returns(Right(())),
        (processor.process _)
          .expects(YaspProcess("id4", Sql("my-sql-2"), None))
          .once()
          .returns(Right(())),
        (writer.write _)
          .expects(YaspSink("id4", Format("parquet", Map("path" -> "destPath1"))))
          .once()
          .returns(Right(())),
        (writer.write _)
          .expects(YaspSink("id3", Format("parquet", Map("path" -> "destPath2"))))
          .once()
          .returns(Right(()))
      )
    )

    yaspExecutor.exec(
      YaspPlan(
        sources = Seq(
          YaspSource(
            id = "id1",
            source = Source.Format("json", options = Map("path" -> "sourcePath1"))
          ),
          YaspSource(
            id = "id2",
            source = Source.Format("parquet", options = Map("path" -> "sourcePath2", "mergeSchema" -> "true"))
          )
        ),
        processes = Seq(
          YaspProcess("id3", Sql("my-sql-1"), None),
          YaspProcess("id4", Sql("my-sql-2"), None)
        ),
        sinks = Seq(
          YaspSink("id4", Format("parquet", Map("path" -> "destPath1"))),
          YaspSink("id3", Format("parquet", Map("path" -> "destPath2")))
        )
      )
    )
  }

}
