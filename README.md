# JobStreamer control bus

The JobStreamer control bus is a message passing system to agents.

## API

You can operate the JobStreamer control bus via web API. It's format is EDN that is popular in clojure.

### List jobs  

```java
GET /jobs
```

#### Response

```clojure
[
  {
    :job/id           "JOB-1"
    :job/restartable? true
  }
]
```

### Create a job

```
POST /jobs
```

#### Parameters

|Name|Type|Description|
|----|----|-----------|
|id|String|Required. The identity of the job.|

#### Example

```clojure
{
  :id "JOB-1"
  :steps [
    {
      :id "STEP-1"
      :batchlet {
        :ref "example.Batchlet"
      }
    }
  ]
}
```

### Get a single job

```
GET /job/:job-id
```

### Update a job

```
PUT /job/:job-id
```

### Delete a job

```
DELETE /job/:job-id
```

### Schedule a job

```
POST /job/:job-id/schedule
```

### Execute a job

```
POST /job/:job-id/executions
```

### List executions

```
GET /job/:job-id/executions
```

### Get a single execution

```
GET /job/:job-id/execution/:execution-id
```

### List agents

```
GET /agents
```

