package io.hydrosphere.sonar.config

import io.hydrosphere.sonar.URLString


/**
 * DO NOT REFACTOR UNLESS YOU WANT TO FIX docker/start.sh FILE
 *
 * @param alertManagerUrl
 * @param frontendUrl
 */
case class AlertingConfiguration(alertManagerUrl: URLString, frontendUrl: URLString)
