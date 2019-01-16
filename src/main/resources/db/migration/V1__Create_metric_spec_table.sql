CREATE TABLE metric_specs(
    kind varchar not null,
    name varchar not null, 
    modelVersionId int not null, 
    config json, 
    withHealth bool,
    id varchar not null primary key
);