package io.hydrosphere.sonar.actors.processors.profiles

import io.hydrosphere.serving.proto.manager.entities.ModelVersion
import io.hydrosphere.serving.proto.contract.types.DataProfileType
import io.hydrosphere.serving.proto.runtime.api.{PredictRequest, PredictResponse}
import io.hydrosphere.monitoring.proto.sonar.entities.ExecutionInformation
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.Processor.ProfileRequest
import io.hydrosphere.sonar.Logging

trait ProfileProcessor extends Logging {
  def filterInputs(modelVersion: ModelVersion, dataType: DataProfileType): Set[String] = {
    val maybeDataTypes = for {
      signature <- modelVersion.signature
    } yield signature.inputs.map(i => (i.name, i.profile)).toMap.filter({ case (_, tpe) => tpe == dataType }).keys.toSet
    maybeDataTypes.getOrElse(Set.empty[String])
  }

  def filterOutputs(modelVersion: ModelVersion, dataType: DataProfileType): Set[String] = {
    val maybeDataTypes = for {
      signature <- modelVersion.signature
    } yield signature.outputs.map(i => (i.name, i.profile)).toMap.filter({ case (_, tpe) => tpe == dataType }).keys.toSet
    maybeDataTypes.getOrElse(Set.empty[String])
  }

  def filterRequest(request: Processor.ProfileRequest, modelVersion: ModelVersion, dataType: DataProfileType): Processor.ProfileRequest = {
    val inputs = filterInputs(modelVersion, dataType)
    val outputs = filterOutputs(modelVersion, dataType)
    ProfileRequest(
      payload = ExecutionInformation(
        request = request.payload.request.map(r => PredictRequest(r.inputs.filter({ case (key, _) => inputs.contains(key) }))),
        metadata = request.payload.metadata,
        responseOrError = if (request.payload.responseOrError.isResponse) {
          request.payload.responseOrError.response match {
            case Some(r) => ExecutionInformation.ResponseOrError.Response(PredictResponse(r.outputs.filter({ case (key, _) => outputs.contains(key) })))
            case None => ExecutionInformation.ResponseOrError.Empty
          }
        } else {
          request.payload.responseOrError
        }
      )
    )
  }
}
