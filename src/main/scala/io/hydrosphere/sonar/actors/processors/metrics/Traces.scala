package io.hydrosphere.sonar.actors.processors.metrics

import io.hydrosphere.serving.monitoring.monitoring.{ExecutionInformation, TraceData}

object Traces {

  def single(ei: ExecutionInformation): String =
    ei.metadata.flatMap(_.traceData) match {
      case Some(traceData) => "\"" + s"${traceData.ts}_${traceData.uid}" + "\""
      case None => "null"
    }

  def many(infos: Seq[ExecutionInformation]): String =
    infos.map(single).mkString("[", ",", "]")

}
