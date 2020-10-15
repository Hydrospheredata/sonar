package io.hydrosphere.sonar.common.config

case class StorageConfiguration(
   bucket: String,
   createBucket: Boolean,
   accessKey: Option[String],
   secretKey: Option[String],
   endpoint: Option[String],
   pathStyleAccess: Option[String],
   s3Impl: Option[String]
)
