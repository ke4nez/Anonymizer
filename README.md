# FEEDBACK

I found this task both engaging and aligned with my interests—specifically, working with data.  
Below is a more detailed overview of the solution and the development process.

## How to Run the Application

To run the application, you can directly execute the `main` function in your IDE, ensuring that the environment variables are provided via the `environment.txt` file.
> **Note:** On production, the application would typically be deployed using Docker. However, for local testing, running it directly via the IDE is sufficient.

To set up a local environment, run the following command to start the necessary services with Docker: `docker-compose up -d`.
## 🕒 Time Spent

Most of the time was invested in exploring technologies that were new to me, particularly **Cap’n Proto** and **ClickHouse**.  
The project was completed over the course of **6 days**, with approximately **4–5 hours of work each day**.

Significant time was dedicated to:
- Researching and evaluating the most optimal solution
- Thorough testing and validation of the implementation

## 🛠️ Implementation

### Kafka Listener

The application continuously listens to a specified Kafka topic and processes message batches at a defined time interval.  
Incoming logs are stored temporarily in memory and then sent to a **ClickHouse** server.

- **Potential Data Loss**:  
  There is a minimal risk of data loss under normal operation. However, if the application is unable to insert data into ClickHouse for an extended period (due to proxy rate limiting or connectivity issues), and the in-memory `dtoList` buffer fills up, older log records may be dropped. The severity of this risk depends on the incoming log rate and the memory limits of the application.

  This issue can be mitigated by adjusting the logic to commit Kafka offsets **only after a successful insert into ClickHouse**. This way, failed insertions will not acknowledge the batch as processed, allowing the application to retry.  
  However, I am not aware of the exact Kafka settings in use, such as retention time and batch size configurations. Because of this, I considered the possibility that uncommitted offsets could lead to Kafka dropping messages. Therefore, I chose to commit the Kafka offsets after reading the messages, rather than after their successful insertion into the database. This ensures that the system can continue processing without the risk of Kafka discarding messages due to uncommitted offsets or retention limits.

⚠️ **Data transfer is limited by a proxy**, which allows only **one request per minute**.

### ClickHouse

Log data is stored in two tables:

- `http_log` – stores raw logs and uses the default **MergeTree** engine.
- `http_log_agg` – stores aggregated logs and uses the **ReplacingMergeTree** engine to deduplicate rows with the same key.  
  This ensures that only the most recent version of a record (e.g., with the highest traffic value) is retained after background merges.

- **Duplicate Records**:  
  Temporary duplicates may appear in the `http_log_agg` table due to how the `ReplacingMergeTree` engine works. These duplicates are cleaned up during background merge operations, which occur asynchronously.  
  A drawback of using `ReplacingMergeTree` is that outdated duplicate records may persist temporarily until a background merge occurs.  
  Due to current limitations (e.g., ClickHouse version and a proxy that restricts queries to one per minute), this is the most optimal solution for now.

  In the future, this can be improved by switching to a **refreshable Materialized View**, which would eliminate the temporary presence of outdated data and provide more precise control over refresh intervals and update logic.

- **Refreshing Aggregated Data**:  
  The aggregated data in the `http_log_agg` table must be refreshed manually by the application. This is currently done by the program at fixed intervals. While this approach works, it is not the most optimal solution. In the future, this process can be improved by using a **refreshable Materialized View**, which would eliminate the need for manual refreshing. A refreshable Materialized View would ensure that aggregated data is always up to date and provide more precise control over refresh intervals and update logic.

### 📊 Data Retention

#### 1. **Average Size of a Single Record**

##### **Table `http_log` (Raw Data):**
Each log entry typically includes the following fields:
- `timestamp` = 8 bytes
- `resource_id` = 8 bytes
- `bytes_sent` = 8 bytes
- `request_time_milli` = 8 bytes
- `response_status` = 2 bytes
- `cache_status` = ~10 bytes (LowCardinality, depends on dictionary size)
- `method` = ~10 bytes
- `remote_addr` = ~16 bytes (for an IPv4 address as string)
- `url` = ~60–150 bytes (typical URL length)

This gives an approximate record size of: **~130–200 bytes per record**.

##### **Table `http_log_agg` (Aggregated Data):**
Aggregated log entries include:
- `resource_id` = 8 bytes
- `response_status` = 2 bytes
- `cache_status` = ~10 bytes
- `remote_addr` = ~16 bytes
- `total_bytes_sent` = 8 bytes
- `request_count` = 8 bytes

This gives an approximate record size of: **~50–80 bytes per record**.

---
# Data Engineering Task

## Task 1 ETL (Anonymizer)

### Requirements

#### Required
* [Docker](https://docs.docker.com/engine/install/)
* [CapNProto](https://capnproto.org/install.html)

#### Recommended
* [Kafka](https://kafka.apache.org/quickstart)
* [clickhouse-client](https://clickhouse.com/docs/en/getting-started/install/)

![](../../anonymizer/anonymizer.png)

Imagine a logging pipeline of HTTP records, data collected from edge
servers and sent to an Apache Kafka topic called `http_log` as messages. Your task consumes
those messages, performs Cap'N Proto decoding, transforms the data as needed, and inserts them
to a ClickHouse table. The Cap'N Proto schema is
[http_log.capnp](../../anonymizer/http_log.capnp).


Because of the GDPR regulations you have to anonymize the client IP. For
each record change `remoteAddr`'s last octet to X before sending it over to
ClickHouse (e.g. 1.2.3.`4` -> 1.2.3.`X`).

* Each record must be stored to ClickHouse, even in the event of network or server error. Make sure
that you handle those appropriately.
* Your application should communicate with the ClickHouse server only through the proxy which has
rate limiting for a 1 request per minute limit.
* If there are any limitation about your application write down what they are, and how would you solve them in the future.
  For example
  * What is the latency of the data?
  * Is there any scenario when you will start losing data?
  * Is it possible that there will be a stored duplicate record?
* You can implement the task in any of those languages:
  * Go
  * C/C++
  * Java
  * Rust

### SQL / ClickHouse part

Load those data into ClickHouse, using a new table called `http_log` with the following columns.

```
  timestamp DateTime
  resource_id UInt64
  bytes_sent UInt64
  request_time_milli UInt64
  response_status UInt16
  cache_status LowCardinality(String)
  method LowCardinality(String)
  remote_addr String
  url String
```

Provide a table with ready made totals of served traffic for any combination of resource ID, HTTP status,
cache status and IP address. The totals need to be queried efficiently, in seconds at best, for on-demand
rendering of traffic charts in a front-end such as Grafana.

Characterize the aggregated select query time to show the table architecture is fit for purpose.
Provide an estimate of disk space required given
 1) average incoming message rate
 2) retention of the aggregated data

### Testing environment

You can use included [docker-compose.yml](../../anonymizer/docker-compose.yml) to setup a local
development environment with every service that you will need for this task.

```bash
$ docker-compose up -d
[+] Running 10/10
 ⠿ Network data-engineering-task_default  Created   0.0s
 ⠿ Container zookeeper                    Started   0.9s
 ⠿ Container prometheus                   Started   0.8s
 ⠿ Container clickhouse                   Started   0.9s
 ⠿ Container ch-proxy                     Started   1.2s
 ⠿ Container broker                       Started   1.3s
 ⠿ Container grafana                      Started   1.2s
 ⠿ Container log-producer                 Started   1.7s
 ⠿ Container kafka-ui                     Started   1.9s
 ⠿ Container jmx-kafka                    Started   1.8s
 ```

After running `docker-compose up -d`, Kafka is available on local port 9092.
You can test it with

```bash
$ kafka-console-consumer --bootstrap-server localhost:9092 \
                         --topic http_log \
                         --group data-engineering-task-reader
```

For testing, a service called `http-log-kafka-producer` is provided, started alongside Kafka.
It produces synthetic Cap'n Proto-encoded records. You can adjust its rate of producing via the
`KAFKA_PRODUCER_DELAY_MS` environment variable inside [docker-compose.yml](../../anonymizer/docker-compose.yml)

ClickHouse should be accessed through a proxy running on HTTP port 8124.

For convenience, [Kafka-UI](localhost:4000/) and [Grafana](localhost:3000/) are running
(user `admin` and password `kafka`) that might be useful if you run into any trouble
with Kafka.
