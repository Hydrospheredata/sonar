package io.hydrosphere.sonar.common.utils

import io.hydrosphere.serving.tensorflow.tensor.{Uint32Tensor, Uint64Tensor, _}
import io.hydrosphere.serving.tensorflow.tensor_shape.TensorShapeProto

object TensorProtoOps {
  
  implicit class TensorProtoConversions(tensorProto: TensorProto) {
    
    def toDoubles: Seq[Double] = TypedTensorFactory.create(tensorProto) match {
      case DoubleTensor(_, data) => data
      case FloatTensor(_, data) => data.map(_.toDouble)
      case integers @ (_: Uint64Tensor | _: Int64Tensor) =>
        integers.data.asInstanceOf[Seq[Long]].map(_.toDouble)
      case integers@(_: Int32Tensor | _: Uint32Tensor | _: Uint8Tensor | _: Int8Tensor | _: Int16Tensor) =>
        integers.data.asInstanceOf[Seq[Int]].map(_.toDouble)
      case BoolTensor(_, data) => data.map(x => if (x) 1.0 else 0.0)

      case unknownTensorType => throw new Exception(s"$unknownTensorType cannot be cast to Seq[Double]")
    }
    
    def toStrings: Seq[String] = TypedTensorFactory.create(tensorProto) match {
      case StringTensor(_, data) => data
      case unknownTensorType => throw new Exception(s"$unknownTensorType cannot be cast to Seq[String]")
    }
    
  }
  
  implicit class TensorShapeProtoGetters(tensorShapeProto: TensorShapeProto) {
    def isScalar: Boolean = tensorShapeProto.dim.isEmpty
  }
}
