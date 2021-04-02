package io.hydrosphere.sonar.utils

import io.hydrosphere.serving.proto.contract.tensor.definitions._
import io.hydrosphere.serving.proto.contract.tensor.Tensor
import io.hydrosphere.serving.proto.contract.tensor.TensorShape
import io.hydrosphere.sonar.Logging

object TensorProtoOps extends Logging {
  
  implicit class TensorProtoConversions(tensorProto: Tensor) {
    
    def toDoubles: Option[Seq[Double]] = TypedTensorFactory.create(tensorProto) match {
      case DoubleTensor(_, data) => Some(data)
      case FloatTensor(_, data) => Some(data.map(_.toDouble))
      case integers @ (_: Uint64Tensor | _: Int64Tensor) =>
        Some(integers.data.asInstanceOf[Seq[Long]].map(_.toDouble))
      case integers@(_: Int32Tensor | _: Uint32Tensor | _: Uint8Tensor | _: Int8Tensor | _: Int16Tensor) =>
        Some(integers.data.asInstanceOf[Seq[Int]].map(_.toDouble))
      case BoolTensor(_, data) => Some(data.map(x => if (x) 1.0 else 0.0))
      case unknownTensorType => {
        logger.warn(s"$unknownTensorType cannot be cast to Seq[Double]")
        None
      }
    }
    
    def toStrings: Option[Seq[String]] = TypedTensorFactory.create(tensorProto) match {
      case StringTensor(_, data) => Some(data)
      case unknownTensorType => {
        logger.warn(s"$unknownTensorType cannot be cast to Seq[String]")
        None
      }
    }
    
  }
  
  implicit class TensorShapeProtoGetters(tensorShapeProto: TensorShape) {
    def isScalar: Boolean = tensorShapeProto.dims.isEmpty
  }
}
