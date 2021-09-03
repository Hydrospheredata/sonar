#!/usr/bin/env sh

JAVA=$(which java)

APP_OPTS=""

[ -z "$JAVA_XMX" ] && JAVA_XMX="1024M"

[[ -z "$GRPC_PORT" ]] && GRPC_PORT="9090"
[[ -z "$GRPC_MAX_SIZE" ]] && GRPC_MAX_SIZE="52428800"

[[ -z "$HTTP_HOST" ]] && HTTP_HOST="0.0.0.0"
[[ -z "$HTTP_PORT" ]] && HTTP_PORT="9091"

[[ -z "$INFLUX_HOST" ]] && INFLUX_HOST="influx"
[[ -z "$INFLUX_PORT" ]] && INFLUX_PORT="8080"
[[ -z "$INFLUX_DATABASE" ]] && INFLUX_DATABASE="sonar-metrics"

[[ -z "$MONGO_HOST" ]] && MONGO_HOST="mongo"
[[ -z "$MONGO_PORT" ]] && MONGO_PORT="27017"
[[ -z "$MONGO_DATABASE" ]] && MONGO_DATABASE="sonar-profiles"
[[ -z "$MONGO_RETRY_WRITES" ]] && MONGO_RETRY_WRITES="true"

[[ -z "$SIDECAR_HOST" ]] && SIDECAR_HOST="sidecar"
[[ -z "$SIDECAR_GRPC_PORT" ]] && SIDECAR_GRPC_PORT="9090"
[[ -z "$SIDECAR_HTTP_PORT" ]] && SIDECAR_HTTP_PORT="9091"

[[ -z "$PROFILE_TEXT_TAGGER_PATH" ]] && PROFILE_TEXT_TAGGER_PATH="/data/models/english-left3words-distsim.tagger"
[[ -z "$PROFILE_TEXT_SHIFT_REDUCE_PARSER_PATH" ]] && PROFILE_TEXT_SHIFT_REDUCE_PARSER_PATH="/data/srparser/englishSR.beam.ser.gz"
[[ -z "$PROFILE_TEXT_LEXPARSER_PATH" ]] && PROFILE_TEXT_LEXPARSER_PATH="/data/lexparser/englishPCFG.ser.gz"
[[ -z "$PROFILE_TEXT_SENTIMENT_PATH" ]] && PROFILE_TEXT_SENTIMENT_PATH="/data/sentiment/sentiment.ser.gz"

[[ -z "${STORAGE_REGION}" ]] && STORAGE_REGION="us-west-1"

[ -z "$JAVA_OPTS" ] && JAVA_OPTS="-Xmx$JAVA_XMX -Xms$JAVA_XMX" 

if [[ "$CUSTOM_CONFIG" = "" ]]
then
    echo "Custom config does not exist"
    APP_OPTS="$APP_OPTS -Dgrpc.port=$GRPC_PORT -Dgrpc.max-size=$GRPC_MAX_SIZE"
    APP_OPTS="$APP_OPTS -Dhttp.host=$HTTP_HOST -Dhttp.port=$HTTP_PORT"
    APP_OPTS="$APP_OPTS -Dinflux.host=$INFLUX_HOST -Dinflux.port=$INFLUX_PORT -Dinflux.database=$INFLUX_DATABASE"
    APP_OPTS="$APP_OPTS -Dmongo.host=$MONGO_HOST -Dmongo.port=$MONGO_PORT -Dmongo.database=$MONGO_DATABASE -Dmongo.retryWrites=$MONGO_RETRY_WRITES"
    [[ ! -z "$MONGO_USER" ]] && APP_OPTS="$APP_OPTS -Dmongo.user=$MONGO_USER"
    [[ ! -z "$MONGO_PASS" ]] && APP_OPTS="$APP_OPTS -Dmongo.pass=$MONGO_PASS"
    [[ ! -z "$MONGO_AUTH_DB" ]] && APP_OPTS="$APP_OPTS -Dmongo.auth-db=$MONGO_AUTH_DB"
    APP_OPTS="$APP_OPTS -Dsidecar.host=$SIDECAR_HOST -Dsidecar.grpc-port=$SIDECAR_GRPC_PORT -Dsidecar.http-port=$SIDECAR_HTTP_PORT"
    APP_OPTS="$APP_OPTS -Dprofile.text.tagger-path=$PROFILE_TEXT_TAGGER_PATH -Dprofile.text.shift-reduce-parser-path=$PROFILE_TEXT_SHIFT_REDUCE_PARSER_PATH -Dprofile.text.lex-parser-path=$PROFILE_TEXT_LEXPARSER_PATH -Dprofile.text.sentiment-path=$PROFILE_TEXT_SENTIMENT_PATH"
    [[ ! -z "$ALERTING_MANAGER_URL" ]] && APP_OPTS="$APP_OPTS -Dalerting.alert-manager-url=$ALERTING_MANAGER_URL"
    [[ ! -z "$ALERTING_FRONTEND_URL" ]] && APP_OPTS="$APP_OPTS -Dalerting.frontend-url=$ALERTING_FRONTEND_URL"
    
    [[ ! -z "$FEATURE_LAKE_BUCKET" ]] && APP_OPTS="$APP_OPTS -Dstorage.bucket=$FEATURE_LAKE_BUCKET"
    [[ ! -z "${STORAGE_ACCESS_KEY}" ]] && APP_OPTS="$APP_OPTS -Dstorage.access-key=${STORAGE_ACCESS_KEY}"
    [[ ! -z "${STORAGE_SECRET_KEY}" ]] && APP_OPTS="$APP_OPTS -Dstorage.secret-key=${STORAGE_SECRET_KEY}"
    [[ ! -z "${STORAGE_REGION}" ]] && APP_OPTS="$APP_OPTS -Dstorage.region=${STORAGE_REGION}"
    [[ ! -z "${STORAGE_ENDPOINT}" ]] && APP_OPTS="$APP_OPTS -Dstorage.endpoint=${STORAGE_ENDPOINT}"
    [[ ! -z "${STORAGE_PATH_STYLE_ACCESS}" ]] && APP_OPTS="$APP_OPTS -Dstorage.path-style-access=${STORAGE_PATH_STYLE_ACCESS}"
    [[ ! -z "${STORAGE_S3_IMPL}" ]] && APP_OPTS="$APP_OPTS -Dstorage.s3-impl=${STORAGE_S3_IMPL}"

    echo "APP_OPTS=$APP_OPTS"
else
   APP_OPTS="$APP_OPTS -Dconfig.file=$CUSTOM_CONFIG"
   echo "with config file config.file=$CUSTOM_CONFIG"
   cat $CUSTOM_CONFIG
fi

echo ${JAVA} ${JAVA_OPTS} -cp "app.jar:./lib/*" ${APP_OPTS} io.hydrosphere.sonar.Main
${JAVA} ${JAVA_OPTS} -cp "app.jar:./lib/*" ${APP_OPTS} io.hydrosphere.sonar.Main
