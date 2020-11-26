package io.hydrosphere.sonar.config

case class StorageConfiguration(
   bucket: String,
   region: Option[String],
   accessKey: Option[String],
   secretKey: Option[String],
   endpoint: Option[String],
   pathStyleAccess: Option[String],
   s3Impl: Option[String]
)
