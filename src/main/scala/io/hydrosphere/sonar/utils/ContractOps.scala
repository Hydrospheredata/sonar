package io.hydrosphere.sonar.utils

import io.hydrosphere.serving.contract.model_contract.ModelContract
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType

object ContractOps {
  def extractInputs(modelContract: ModelContract) = {
    modelContract.predict.map(_.inputs).getOrElse(Seq.empty)
  }

  def extractOutputs(modelContract: ModelContract) = {
    modelContract.predict.map(_.outputs).getOrElse(Seq.empty)
  }

  def extractAllFields(modelContract: ModelContract) = {
    extractInputs(modelContract) ++ extractOutputs(modelContract)
  }

  def extractProfiledFields(modelContract: ModelContract) = {
    extractAllFields(modelContract).filter(_.profile != DataProfileType.NONE)
  }
}