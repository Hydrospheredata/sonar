package io.hydrosphere.sonar.services

import io.hydrosphere.sonar.config.Configuration
import io.minio.MinioClient

object S3Client {
  final val AWS_S3_ENDPOINT = "https://s3.amazonaws.com"

  def fromConfig(config: Configuration): MinioClient = {
    val endpoint = config.storage.endpoint.getOrElse(AWS_S3_ENDPOINT)
    val maybeMinio = for {
      accessKey <- config.storage.accessKey
      secretKey <- config.storage.secretKey
      region <- config.storage.region
    } yield new MinioClient(endpoint, accessKey, secretKey, region)
    maybeMinio.getOrElse(new MinioClient(endpoint))
  }
}
