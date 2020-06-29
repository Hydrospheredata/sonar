package io.hydrosphere

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.{Uri, Url}
import org.slf4j.{Logger, LoggerFactory}

package object sonar {
  type URLString = String Refined Url

  trait Logging {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
  }

}
