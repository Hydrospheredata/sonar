package io.hydrosphere.sonar.utils

object CollectionOps {

  def safeTranspose[A](matrix: Seq[Seq[A]]): Seq[Seq[A]] = {
    if (matrix.isEmpty) {
      Seq.empty[Seq[A]]
    } else {
      val maxSize = matrix.map(_.size).max
      matrix.filter(_.size == maxSize).transpose
    }
  }
  
}
