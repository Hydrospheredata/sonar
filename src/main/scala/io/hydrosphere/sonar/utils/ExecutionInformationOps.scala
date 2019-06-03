package io.hydrosphere.sonar.utils

import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.utils.TensorProtoOps._

object ExecutionInformationOps extends Logging {
  
  implicit class ExecutionInformationGetters(ei: ExecutionInformation) {
    def getDoubleInput(input: String): Seq[Double] = {
      val maybeFlat = for {
        r <- ei.request
        input <- r.inputs.get(input)
      } yield input.toDoubles
      maybeFlat.getOrElse(Seq.empty)
    }
    
    def getStringInput(input: String): Seq[String] = {
      val maybeFlat = for {
        r <- ei.request
        input <- r.inputs.get(input)
      } yield input.toStrings
      maybeFlat.getOrElse(Seq.empty)
    }
    
    def getTimestamp: Long = {
      val maybeOriginTimestamp = for {
        metadata <- ei.metadata
        traceData <- metadata.originTraceData
      } yield traceData.ts
      
      val maybeTimestamp = for {
        metadata <- ei.metadata
        traceData <- metadata.traceData
      } yield traceData.ts
      
      (maybeOriginTimestamp, maybeTimestamp) match {
        case (Some(ts), _) => ts
        case (None, Some(ts)) => ts
        case (None, None) => System.currentTimeMillis()
      }
    }
  }
  
}
