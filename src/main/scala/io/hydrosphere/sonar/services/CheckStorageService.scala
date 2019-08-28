package io.hydrosphere.sonar.services

import io.hydrosphere.sonar.terms.Check

trait CheckStorageService[F[_]] {
  def saveChecks(check: Seq[Check]): F[Unit]
  
}
