package io.hydrosphere.sonar.actors.processors.profiles

import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.serving.tensorflow.api.predict.PredictRequest
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.Processor.ProfileRequest

trait ProfileProcessor {
  def numericalInputs(modelVersion: ModelVersion, dataType: DataProfileType): Set[String] = {
    modelVersion.contract.head.predict.get.inputs.head.profile
    val maybeDataTypes = for {
      contract <- modelVersion.contract
      predict <- contract.predict
    } yield predict.inputs.map(i => (i.name, i.profile)).toMap.filter({ case (_, tpe) => tpe == dataType }).keys.toSet
    maybeDataTypes.getOrElse(Set.empty[String])
  }

  def filterRequest(request: Processor.ProfileRequest, modelVersion: ModelVersion, dataType: DataProfileType): Processor.ProfileRequest = {
    val inputs = numericalInputs(modelVersion, dataType)
    ProfileRequest(
      payload = ExecutionInformation(
        request = request.payload.request.map(r => PredictRequest(r.modelSpec, r.inputs.filter({ case (key, _) => inputs.contains(key) }))),
        metadata = request.payload.metadata,
        responseOrError = ExecutionInformation.ResponseOrError.Empty
      )
    )
  }
}
