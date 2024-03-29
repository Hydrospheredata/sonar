package io.hydrosphere.sonar.utils

import io.hydrosphere.serving.proto.contract.field.ModelField
import io.hydrosphere.serving.proto.contract.field.ModelField.TypeOrSubfields
import io.hydrosphere.serving.proto.contract.types.DataType

object ModelFieldOps {
  
  implicit class ModelFieldGetters(modelField: ModelField) {
    def eitherSubfieldOrDataType: Either[ModelField.Subfield, DataType] =
      modelField.typeOrSubfields match {
        case TypeOrSubfields.Empty => throw new RuntimeException("Type of ModelField is empty")
        case TypeOrSubfields.Subfields(value) => Left(value)
        case TypeOrSubfields.Dtype(value) => Right(value)
      }
  }
  
}
