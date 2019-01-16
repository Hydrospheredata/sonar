package io.hydrosphere.sonar.utils

object CollectionOps {

  def safeTranspose(matrix: Seq[Seq[Double]]): Seq[Seq[Double]] = {
    if (matrix.isEmpty) {
      Seq.empty[Seq[Double]]
    } else {
      val maxSize = matrix.map(_.size).max
      matrix.filter(_.size == maxSize).transpose
    }
  }
  
}
