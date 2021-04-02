package io.hydrosphere.sonar.utils

import io.hydrosphere.serving.proto.contract.field.ModelField
import io.hydrosphere.serving.proto.contract.signature.ModelSignature
import io.hydrosphere.serving.proto.contract.types.DataProfileType

object SignatureOps {
  
  implicit class SignatureGetters(signature: ModelSignature) {
    def extractInputs: Seq[ModelField] = signature.inputs

    def extractOutputs: Seq[ModelField] = signature.outputs

    def extractAllFields: Seq[ModelField] = {
      extractInputs ++ extractOutputs
    }

    def extractProfiledFields: Seq[ModelField] = {
      extractAllFields.filter(_.profile != DataProfileType.NONE)
    } 
  }
}