package io.hydrosphere.sonar.utils

import io.hydrosphere.serving.contract.model_contract.ModelContract
import io.hydrosphere.serving.contract.model_field.ModelField
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType

object ContractOps {
  
  implicit class ContractGetters(modelContract: ModelContract) {
    def extractInputs: Seq[ModelField] = {
      modelContract.predict.map(_.inputs).getOrElse(Seq.empty)
    }

    def extractOutputs: Seq[ModelField] = {
      modelContract.predict.map(_.outputs).getOrElse(Seq.empty)
    }

    def extractAllFields: Seq[ModelField] = {
      extractInputs ++ extractOutputs
    }

    def extractProfiledFields: Seq[ModelField] = {
      extractAllFields.filter(_.profile != DataProfileType.NONE)
    } 
  }
}