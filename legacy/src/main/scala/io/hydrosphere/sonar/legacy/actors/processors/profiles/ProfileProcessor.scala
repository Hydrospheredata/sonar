package io.hydrosphere.sonar.legacy.actors.processors.profiles

import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.serving.tensorflow.api.predict.PredictRequest
import io.hydrosphere.sonar.legacy.actors.Processor
import io.hydrosphere.sonar.legacy.actors.Processor.ProfileRequest
import io.hydrosphere.sonar.common.utils.Logging
import io.hydrosphere.serving.tensorflow.api.predict.PredictResponse

trait ProfileProcessor extends Logging {
  def filterInputs(modelVersion: ModelVersion, dataType: DataProfileType): Set[String] = {
    val maybeDataTypes = for {
      contract <- modelVersion.contract
      predict <- contract.predict
    } yield predict.inputs.map(i => (i.name, i.profile)).toMap.filter({ case (_, tpe) => tpe == dataType }).keys.toSet
    maybeDataTypes.getOrElse(Set.empty[String])
  }

  def filterOutputs(modelVersion: ModelVersion, dataType: DataProfileType): Set[String] = {
    val maybeDataTypes = for {
      contract <- modelVersion.contract
      predict <- contract.predict
    } yield predict.outputs.map(i => (i.name, i.profile)).toMap.filter({ case (_, tpe) => tpe == dataType }).keys.toSet
    maybeDataTypes.getOrElse(Set.empty[String])
  }

  def filterRequest(request: Processor.ProfileRequest, modelVersion: ModelVersion, dataType: DataProfileType): Processor.ProfileRequest = {
    val inputs = filterInputs(modelVersion, dataType)
    val outputs = filterOutputs(modelVersion, dataType)
    ProfileRequest(
      payload = ExecutionInformation(
        request = request.payload.request.map(r => PredictRequest(r.modelSpec, r.inputs.filter({ case (key, _) => inputs.contains(key) }))),
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
