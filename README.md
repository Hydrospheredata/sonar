# Sonar2

## API

### Create Metric Specification

```
POST /monitoring/metricspec
{
    "name": string,
    "modelVersionId": long,
    "config": { <kind specific config> },
    "withHealth": boolean (optional),
    "kind": string
}

200 OK
{
    "name": string,
    "modelVersionId": long,
    "config": { <specific config> },
    "withHealth": boolean (optional),
    "kind": string,
    "id" : string
}
```

#### Metric Specification kinds and their configs

- `KSMetricSpec` – Kolmogorov-Smirnov test
```
{
    "input": string
}
```

- `RFMetricSpec` – Random Forest (through serving app)
```
{
    "input": string,
    "applicationName": string,
    "threshold": double (optional, only for health)
}
```

- `AEMetricSpec` – Autoencoder (through serving app)
```
{
    "input": string,
    "applicationName": string,
    "threshold": double (optional, only for health)
}
```

- `imageAEMetricSpec` – Image Autoencoder
```
{
    "applicationName": string,
    "threshold": double (optional, only for health)
}
```

- `GANMetricSpec` – GAN (through serving app)
```
{
    "input": string,
    "applicationName": string,
    "applicationSignature": string
}
```

- `LatencyMetricSpec` – Request Latency
```
{
    "interval": long (in seconds),
    "threshold": double (optional, only for health) 
}
```

- `CounterMetricSpec` – Request Counter
```
{
    "interval": long (in seconds)
}
```

- `ErrorRateMetricSpec` – Error Rate
```
{
    "interval": long (in seconds),
    "threshold": double (optional, only for health) 
}
```

- `AccuracyMetricSpec` – Prediction Accuracy
```
{

}
```

### Get Metric Specification

#### By ID

```
GET /monitoring/metricspec/{metric-spec-uuid}

200 OK
{
    "name": string,
    "modelVersionId": long,
    "config": { <specific config> },
    "withHealth": boolean (optional),
    "kind": string
    "id": string
}
```

#### By Model Version

```
GET /monitoring/metricspec/modelversion/{model-version-id}

200 OK
[
    {
        "name": string,
        "modelVersionId": long,
        "config": { <specific config> },
        "withHealth": boolean (optional),
        "kind": string
        "id": string,
    },
    ...
]
```

#### All

```
GET /monitoring/metricspec

200 OK
[
    {
        "name": string,
        "modelVersionId": long,
        "config": { <specific config> },
        "withHealth": boolean (optional),
        "kind": string
        "id": string,
    },
    ...
]
```

### Get Metrics

```
GET /monitoring/metrics?modelVersionId=<long>&interval=<long>&metrics=<string, repeatable>&columnIndex=<string, optional>

200 OK
[
    {
        "name": string,
        "value": double,
        "labels": {
            "modelVersionId": string
        },
        "health": int (0 or 1, nullable),
        "timestamp": long (seconds)
    },
    ...
]
```

#### Metric Names

- Kolmogorov-Smirnov
    - `kolmogorovsmirnov`
    - `kolmogorovsmirnov_level`
- Autoencoder
    - `autoencoder_reconstructed`
- Random Forest
    - `randomforest`
- GAN
    - `gan_outlier`
    - `gan_inlier`
- Request Latency
    - `latency`
- Request Counter
    - `counter`
- Error Rate
    - `error_rate`
- Prediction Accuracy
    - `accuracy`

### Get Profiles

```
GET /monitoring/profiles/{model-version-id}/{field-name}

200 OK
{
    "training": {<type specific profile>} (nullable),
    "production": {<type specific profile>} (nullable)
}
```

#### Profile Types

- NumericalProfile
```
{
    "name": string (field name),
    "modelVersionId": long,
    "commonStatistics": {
        "count": long,
        "distinctCount": long,
        "missing": long
    },
    "quantileStatistics": {
        "min": double,
        "max": double,
        "median": double,
        "percentile5": double,
        "percentile95": double,
        "q1": double,
        "q3": double,
        "range": double,
        "interquartileRange": double
    },
    "descriptiveStatistics": {
        "standardDeviation": double,
        "variationCoef": double,
        "kurtosis": double,
        "mean": double,
        "skewness": double (nullable),
        "variance": double
    },
    "histogram": {
        "min": double,
        "max": double,
        "step": double,
        "bars": int,
        "frequencies": [
            int,
            ...
        ],
        "bins": [
            double,
            ...
        ]
    },
    "kind":"NumericalProfile"
    }
}
```

#### Getting field names

```
GET /monitoring/fields/{model-version-id}

200 OK
[
    string,
    ...
]
```

### Training data upload

#### Start processing

```
POST /monitoring/profiles/batch/{model-version-id}
Transfer-Encoding: chunked
<chunked csv-body>

200 OK
"ok"
```

#### Get processing status

```
GET /monitoring/profiles/batch/{model-version-id}/status

200 OK
{
    "kind": string
}
```

##### Statuses:
- Success
- Failure
- Processing
- NotRegistered


#### Get training data for a model

```
GET /monitoring/training_data?modelVersionId={model-version-id}

200 OK
[
    "s3://bucket/data1.csv",
    "s3://bucket/data2.csv"
]
```
