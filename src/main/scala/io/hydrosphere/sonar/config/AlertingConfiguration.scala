package io.hydrosphere.sonar.config

/**
 * DO NOT REFACTOR UNLESS YOU WANT TO FIX docker/start.sh FILE
 * @param alertManagerUrl
 * @param frontendUrl
 */
case class AlertingConfiguration(alertManagerUrl: String, frontendUrl: String)
