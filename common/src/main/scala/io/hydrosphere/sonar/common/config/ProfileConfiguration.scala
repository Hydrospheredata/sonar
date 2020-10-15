package io.hydrosphere.sonar.common.config

case class ProfileConfiguration(text: TextConfiguration)

case class TextConfiguration(taggerPath: String, shiftReduceParserPath: String, lexParserPath: String, sentimentPath: String)
