package io.hydrosphere.sonar

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.Url

package object common {
  type URLString = String Refined Url
}
