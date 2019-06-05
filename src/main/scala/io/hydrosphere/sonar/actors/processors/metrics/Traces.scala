package io.hydrosphere.sonar.actors.processors.metrics

import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.serving.monitoring.metadata.{ExecutionMetadata, TraceData}


object AbstractTraces {
  def single(f: ExecutionMetadata => Option[TraceData])(ei: ExecutionInformation): Seq[Option[TraceData]] = many(f)(Seq(ei))

  def many(f: ExecutionMetadata => Option[TraceData])(infos: Seq[ExecutionInformation]): Seq[Option[TraceData]] = infos.map(_.metadata.flatMap(f))
  //  def single(ei: ExecutionInformation): String =
//    ei.metadata.flatMap(_.traceData) match {
//      case Some(traceData) => "\"" + s"${traceData.ts}_${traceData.uid}" + "\""
//      case None => "null"
//    }
//  def many(infos: Seq[ExecutionInformation]): String =
//    infos.map(single).mkString("[", ",", "]")

}

object Traces {
  def single(ei: ExecutionInformation): Seq[Option[TraceData]] = AbstractTraces.single(_.traceData)(ei)
  def many(infos: Seq[ExecutionInformation]): Seq[Option[TraceData]] = AbstractTraces.many(_.traceData)(infos)
}

object OriginTraces {
  def single(ei: ExecutionInformation): Seq[Option[TraceData]] = AbstractTraces.single(_.originTraceData)(ei)
  def many(infos: Seq[ExecutionInformation]): Seq[Option[TraceData]] = AbstractTraces.many(_.originTraceData)(infos)
}
