#!/usr/bin/env sh

JAVA=$(which java)

APP_OPTS=""

[[ -z "$DB_TYPE" ]] && DB_TYPE="h2"
[[ -z "$DB_JDBC_URL" ]] && DB_JDBC_URL="jdbc:h2:file:./target/db.h2;DB_CLOSE_DELAY=-1;INIT=create domain if not exists json as other;"
[[ -z "$DB_USER" ]] && DB_USER="sa"
[[ -z "$DB_PASS" ]] && DB_PASS=""

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

[[ -z "$SIDECAR_HOST" ]] && SIDECAR_HOST="sidecar"
[[ -z "$SIDECAR_GRPC_PORT" ]] && SIDECAR_GRPC_PORT="9090"
[[ -z "$SIDECAR_HTTP_PORT" ]] && SIDECAR_HTTP_PORT="9091"

[[ -z "$SUBSAMPLING_TYPE" ]] && SUBSAMPLING_TYPE="reservoir"
[[ -z "$SUBSAMPLING_SIZE" ]] && SUBSAMPLING_SIZE="1000"

if [[ "$CUSTOM_CONFIG" = "" ]]
then
    echo "Custom config does not exist"
    APP_OPTS="$APP_OPTS -Ddb.type=$DB_TYPE -Ddb.jdbc-url=$DB_JDBC_URL -Ddb.user=$DB_USER -Ddb.pass=$DB_PASS"
    APP_OPTS="$APP_OPTS -Dgrpc.port=$GRPC_PORT -Dgrpc.max-size=$GRPC_MAX_SIZE"
    APP_OPTS="$APP_OPTS -Dhttp.host=$HTTP_HOST -Dhttp.port=$HTTP_PORT"
    APP_OPTS="$APP_OPTS -Dinflux.host=$INFLUX_HOST -Dinflux.port=$INFLUX_PORT -Dinflux.database=$INFLUX_DATABASE"
    APP_OPTS="$APP_OPTS -Dmongo.host=$MONGO_HOST -Dmongo.port=$MONGO_PORT -Dmongo.database=$MONGO_DATABASE"
    [[ ! -z "$MONGO_USER" ]] && APP_OPTS="$APP_OPTS -Dmongo.user=$MONGO_USER"
    [[ ! -z "$MONGO_PASS" ]] && APP_OPTS="$APP_OPTS -Dmongo.pass=$MONGO_PASS"
    [[ ! -z "$MONGO_AUTH_DB" ]] && APP_OPTS="$APP_OPTS -Dmongo.auth-db=$MONGO_AUTH_DB"
    APP_OPTS="$APP_OPTS -Dsidecar.host=$SIDECAR_HOST -Dsidecar.grpc-port=$SIDECAR_GRPC_PORT -Dsidecar.http-port=$SIDECAR_HTTP_PORT"
    APP_OPTS="$APP_OPTS -Dsubsampling.type=$SUBSAMPLING_TYPE -Dsubsampling.size=$SUBSAMPLING_SIZE"

    echo "APP_OPTS=$APP_OPTS"
else
   APP_OPTS="$APP_OPTS -Dconfig.file=$CUSTOM_CONFIG"
   echo "with config file config.file=$CUSTOM_CONFIG"
   cat $CUSTOM_CONFIG
fi

${JAVA} -cp "/app/app.jar:/app/lib/*" ${APP_OPTS} io.hydrosphere.sonar.Main
