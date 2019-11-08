package io.hydrosphere.sonar.utils

object BooleanOps {
  implicit class BooleanConversions(value: Boolean) {
    def toInt: Int = if (value) 1 else 0
  }
}
