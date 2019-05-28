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
  }
  
}
