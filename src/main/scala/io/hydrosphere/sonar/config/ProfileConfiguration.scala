package io.hydrosphere.sonar.config

case class ProfileConfiguration(text: TextConfiguration)

case class TextConfiguration(taggerPath: String, shiftReduceParserPath: String, lexParserPath: String, sentimentPath: String)
