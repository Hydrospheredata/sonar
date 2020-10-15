package io.hydrosphere.sonar.common.utils

import io.hydrosphere.serving.contract.model_field.ModelField
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.tensorflow.types.DataType
import org.apache.avro.{Schema, SchemaBuilder}
import TensorProtoOps._
import ModelFieldOps._

object ParquetHelper {
  def inferSchema(modelFields: Seq[ModelField]): Schema = {
    var rootBuilder =
      SchemaBuilder
        .record("HydroServingRequestResponse").namespace("io.hydrosphere.serving.requestresponse")
        .fields()
        .requiredString("_id")
        .requiredDouble("_hs_latency")
        .requiredLong("_hs_model_version_id")
        .requiredString("_hs_model_name")
        .requiredLong("_hs_model_incremental_version")
        .requiredString("_hs_request_id")
        .requiredLong("_hs_timestamp")
        .requiredInt("_hs_year")
        .requiredInt("_hs_month")
        .requiredInt("_hs_day")

        .optionalString("_hs_error")
        .optionalDouble("_hs_score")
        .optionalDouble("_hs_overall_score")

    modelFields.foreach(modelField => {
      modelField.eitherSubfieldOrDataType match {
        case Left(_) => // do nothing 
        case Right(dataType) =>
          def setType(scalarFn: SchemaBuilder.BaseFieldTypeBuilder[Schema] => SchemaBuilder.FieldAssembler[Schema], arrayFn: SchemaBuilder.TypeBuilder[SchemaBuilder.ArrayDefault[Schema]] => SchemaBuilder.ArrayDefault[Schema]): SchemaBuilder.FieldAssembler[Schema] = {
            if (modelField.shape.get.isScalar) {
              scalarFn(rootBuilder.name(modelField.name).`type`().nullable())
            } else {
              arrayFn(rootBuilder.name(modelField.name).`type`().array().items()).noDefault()
            }
          }

          rootBuilder = dataType match {
            case DataType.DT_INVALID => rootBuilder // do nothing
            case DataType.DT_FLOAT => setType(_.floatType().noDefault(), _.floatType())
            case DataType.DT_DOUBLE => setType(_.doubleType().noDefault(), _.doubleType())
            case DataType.DT_INT32 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_UINT8 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_INT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_INT8 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_STRING => setType(_.stringType().noDefault(), _.stringType())
            case DataType.DT_COMPLEX64 => setType(_.doubleType().noDefault(), _.doubleType())
            case DataType.DT_INT64 => setType(_.longType().noDefault(), _.longType())
            case DataType.DT_BOOL => setType(_.booleanType().noDefault(), _.booleanType())
            case DataType.DT_QINT8 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_QUINT8 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_QINT32 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_BFLOAT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_QINT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_QUINT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_UINT16 => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_COMPLEX128 => setType(_.doubleType().noDefault(), _.doubleType())
            case DataType.DT_HALF => setType(_.intType().noDefault(), _.intType())
            case DataType.DT_RESOURCE => rootBuilder // do nothing
            case DataType.DT_VARIANT => rootBuilder // do nothing
            case DataType.DT_UINT32 => setType(_.longType().noDefault(), _.longType())
            case DataType.DT_UINT64 => setType(_.longType().noDefault(), _.longType())
            case DataType.DT_MAP => rootBuilder // do nothing
            case DataType.Unrecognized(value) => rootBuilder // do nothing
          }
          rootBuilder = rootBuilder.name(s"_hs_${modelField.name}_score").`type`().nullable().doubleType().noDefault()
      }
    })

    rootBuilder = rootBuilder.name("_hs_metric_checks").`type`().map().values().nullable().record("RawCheck").fields()
      .requiredBoolean("check")
      .requiredString("description")
      .requiredDouble("threshold")
      .requiredDouble("value")
      .optionalString("metricSpecId")
      .endRecord().noDefault()

    var rawChecksBuilder = rootBuilder.name("_hs_raw_checks").`type`().record("_hs_raw_checks").fields()

    modelFields.foreach(modelField => {
      modelField.profile match {
        case DataProfileType.NUMERICAL =>
          rawChecksBuilder = rawChecksBuilder
            .name(modelField.name).`type`().array().items().`type`("RawCheck").noDefault()
        case DataProfileType.NONE => // do nothing
        case DataProfileType.CATEGORICAL => // do nothing
        case DataProfileType.NOMINAL => // do nothing
        case DataProfileType.ORDINAL => // do nothing
        case DataProfileType.CONTINUOUS => // do nothing
        case DataProfileType.INTERVAL => // do nothing
        case DataProfileType.RATIO => // do nothing
        case DataProfileType.IMAGE => // do nothing
        case DataProfileType.VIDEO => // do nothing
        case DataProfileType.AUDIO => // do nothing
        case DataProfileType.TEXT => // do nothing
        case DataProfileType.Unrecognized(value) => // do nothing
      }
    })

    rootBuilder = rawChecksBuilder.endRecord().noDefault()

    rootBuilder.endRecord()
  }
}
