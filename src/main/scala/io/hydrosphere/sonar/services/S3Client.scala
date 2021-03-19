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
    } yield {
      // Null is scary but it's a part of their API.
      // They have overloaded ctor for (endpoint, accessKey, secretKey) so null is acceptable.
      // see MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
      new MinioClient(endpoint, accessKey, secretKey, config.storage.region.orNull)
    }

    maybeMinio.getOrElse(new MinioClient(endpoint))
  }
}
