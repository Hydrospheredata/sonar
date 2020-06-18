package io.hydrosphere.sonar.config

import io.hydrosphere.sonar.URLString

// TODO(bulat): alertManagerUrl is actually URI. Finagle client doesn't accept protocol in Service constructor
/**
 * DO NOT REFACTOR UNLESS YOU WANT TO FIX docker/start.sh FILE
 *
 * @param alertManagerUrl
 * @param frontendUrl
 */
case class AlertingConfiguration(alertManagerUrl: String, frontendUrl: URLString)
