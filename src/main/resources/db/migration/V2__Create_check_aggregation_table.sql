CREATE TABLE check_aggregations(
    modelVersionId int not null,
    feature varchar not null,
    checks int not null default 0,
    passed int not null default 0,
    requests int not null default 0,
    first_timestamp int not null,
    first_uid int not null,
    last_timestamp int not null,
    last_uid int not null
);

CREATE INDEX modelVersionId ON check_aggregations(modelVersionId DESC);

CREATE TABLE check_aggregations_prometheus(
    modelVersionId int not null,
    feature varchar not null,
    checks int not null default 0,
    passed int not null default 0,
    requests int not null default 0,
    first_timestamp int not null,
    first_uid int not null,
    last_timestamp int not null,
    last_uid int not null
);

CREATE INDEX modelVersionId ON check_aggregations_prometheus(modelVersionId DESC);