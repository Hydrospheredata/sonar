package io.hydrosphere

import org.slf4j.{Logger, LoggerFactory}

package object sonar {

  trait Logging {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
  }

}
