## KsqlDB

Eventhub kafka sidecar support both Active and Reactive ksqldb consumer

### Reactive consumer

Start with KsqldbReactiveConsumerStartupHook:

```text
# Service Startup and Shutdown Hooks
service.com.networknt.server.StartupHookProvider:
  - com.networknt.mesh.kafka.ProducerStartupHook
  # - com.networknt.mesh.kafka.ReactiveConsumerStartupHook
  - com.networknt.mesh.kafka.KsqldbActiveConsumerStartupHook
  - com.networknt.mesh.kafka.ReactiveConsumerStartupHook
  # - com.networknt.mesh.kafka.AdminClientStartupHook
service.com.networknt.server.ShutdownHookProvider:
  - com.networknt.mesh.kafka.ProducerShutdownHook
  # - com.networknt.mesh.kafka.ReactiveConsumerShutdownHook
  - com.networknt.mesh.kafka.ReactiveConsumerShutdownHook
  # - com.networknt.mesh.kafka.AdminClientShutdownHook
  - com.networknt.mesh.kafka.KsqldbActiveConsumerShutdownHook

```

//TODO for the detail


### Active consumer

Start with :

```text
# Service Startup and Shutdown Hooks
service.com.networknt.server.StartupHookProvider:
  - com.networknt.mesh.kafka.ProducerStartupHook
  # - com.networknt.mesh.kafka.ActiveConsumerStartupHook
  # - com.networknt.mesh.kafka.KsqldbConsumerStartupHook
  - com.networknt.mesh.kafka.KsqldbActiveConsumerStartupHook
  - com.networknt.mesh.kafka.ReactiveConsumerStartupHook
  # - com.networknt.mesh.kafka.AdminClientStartupHook
service.com.networknt.server.ShutdownHookProvider:
  - com.networknt.mesh.kafka.ProducerShutdownHook
  # - com.networknt.mesh.kafka.ActiveConsumerShutdownHook
  # - com.networknt.mesh.kafka.KsqldbConsumerShutdownHook
  - com.networknt.mesh.kafka.ReactiveConsumerShutdownHook
  # - com.networknt.mesh.kafka.AdminClientShutdownHook
  - com.networknt.mesh.kafka.KsqldbActiveConsumerShutdownHook

```

KsqldbActiveConsumerStartupHook will initial a kafka API client based the kafka-ksqldb.yml config.

For local connection, it only need host and port for connection:

```text
ksqldbHost: ${kafka-ksqldb.ksqldbHost:localhost}
# ksqlDB port
ksqldbPort: ${kafka-ksqldb.ksqldbPort:8088}
```

For Enterprise Kafka KSQL server, we need use tls connection and use the base Authentication:

```text
useTls: ${kafka-ksqldb.useTls:false}
trustStore: ${kafka-ksqldb.trustStore:/truststore/kafka.server.truststore.jks}
trustStorePassword: ${kafka-ksqldb.trustStorePassword:changeme}
basicAuthCredentialsUser: ${kafka-ksqldb.basicAuthCredentialsUser:userId}
basicAuthCredentialsPassword: ${kafka-ksqldb.basicAuthCredentialsPassword:changeme}
```

There is new endpoint added for executing ksqlDB query:


```text
  '/ksqldb/active':
    post:
      operationId: KsqlDBPullQueryActive
      summary: KsqlDBPullQuery APIs by active consumer
      requestBody:
        description: "process a ksqlDB query"
        required: true
        content:
          application/json:
            schema:
              "$ref": "#/components/schemas/KsqlDbPullQueryRequest"
      responses:
        '200':
          description: Successful response
          content:
            application/json:
              schema:
                type: object
```
There are two types of queries in ksqldb:

- pull
- push

Please refer [here](https://docs.ksqldb.io/en/latest/concepts/queries/) for detail.

"/ksqldb/active" endpoint support both query types, but we suggest to use pull query for this endpoint only. It much stable and have better performance.


There is new request object been added into light-kafka:

https://github.com/networknt/light-kafka/blob/master/kafka-entity/src/main/java/com/networknt/kafka/entity/KsqlDbPullQueryRequest.java

Sample request payload:

```text
{
    "offset": "earliest",
    "deserializationError": false,
    "queryType": "pull",
     "tableScanEnable": true,
    "query": "select * from QUERYUSER1;"
}

```

 - offset
   optional field, only use for push query. Available values: earliest/latest

- queryType
  optional field, indicate query type. Available values: pull/push

- deserializationError
  optional field, indicates whether to fail if corrupt messages are read. Available values: true/false

- tableScanEnable
  optional field, indicates whether full table scan allowed. Available values: true/false

- query
  required field, ksqlDB query string


#### Sample request:

```text
curl --location --request POST 'http://localhost:8084/ksqldb/active' \
--header 'Content-Type: application/json' \
--data-raw ' 
{
    "offset": "earliest",
    "deserializationError": false,
    "queryType": "pull",
     "tableScanEnable": true,
    "query": "select * from QUERYUSER1 where id = '\''1'\'';"
}
'
```

Response:

```text
[
    {
        "USERID": "4444",
        "FIRSTNAME": "test1",
        "ID": "1"
    }
]
```