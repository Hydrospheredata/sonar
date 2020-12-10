package io.hydrosphere.sonar.utils

import io.hydrosphere.serving.tensorflow.tensor.{Uint32Tensor, Uint64Tensor, _}
import io.hydrosphere.serving.tensorflow.tensor_shape.TensorShapeProto

object TensorProtoOps {
  
  implicit class TensorProtoConversions(tensorProto: TensorProto) {
    
    def toDoubles: Option[Seq[Double]] = TypedTensorFactory.create(tensorProto) match {
      case DoubleTensor(_, data) => Some(data)
      case FloatTensor(_, data) => Some(data.map(_.toDouble))
      case integers @ (_: Uint64Tensor | _: Int64Tensor) =>
        Some(integers.data.asInstanceOf[Seq[Long]].map(_.toDouble))
      case integers@(_: Int32Tensor | _: Uint32Tensor | _: Uint8Tensor | _: Int8Tensor | _: Int16Tensor) =>
        Some(integers.data.asInstanceOf[Seq[Int]].map(_.toDouble))
      case BoolTensor(_, data) => Some(data.map(x => if (x) 1.0 else 0.0))
      case _ => None
    }
    
    def toStrings: Option[Seq[String]] = TypedTensorFactory.create(tensorProto) match {
      case StringTensor(_, data) => Some(data)
      case _ => None
    }
    
  }
  
  implicit class TensorShapeProtoGetters(tensorShapeProto: TensorShapeProto) {
    def isScalar: Boolean = tensorShapeProto.dim.isEmpty
  }
}
